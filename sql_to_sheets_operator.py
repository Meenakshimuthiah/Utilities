import uuid

from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


# pylint: disable=R0903
class SqlToSheetsOperator(BaseOperator):
    """
    Copies data from SQL Queries to Worksheets in a Google Sheets Spreadsheet.
    All the Worksheets must be in the same Spreadsheet.

    Edits a Google Sheets Spreadsheet using the Google Drive API v3 and
    Google Sheets API v4.
    The account used in the hook must have access to the Spreadsheet that is referenced.

    :param worksheetSqlMap: A Map of Worksheet names to SQL Queries. The results of the SQL
        queries replace the contents of Worksheet it is mapped to.
    :type worksheetSqlMap: dict[str] (templated)
    :param spreadsheetId: The ID of the Google Sheets Spreadsheet.
        (It is usually displayed at the end of the link of the Spreadsheet)
    :type spreadsheetId: str (templated)
    :param copyTo: The string appended to the copy of the referenced Spreadsheet.
        A copy of a file will be made if this is specified, otherwise the given Spreadsheet
        is edited.
    :type copyTo: str
    :param parentId: The folder ID of the parent folder if there is one. This is only applicable
        when the file is being copied. If the parentId is not specified, the parent of the copied
        file will be the same as that of the original file.
    :type parentId: str
    """
    template_fields = ('worksheetSqlMap', 'spreadsheetId', 'copyTo')

    @apply_defaults
    def __init__(self,
                 worksheetSqlMap,
                 spreadsheetId,
                 copyTo=None,
                 parentId=None,
                 *args, **kwargs
                 ):
        super(SqlToSheetsOperator, self).__init__(*args, **kwargs)
        self.worksheetSqlMap = worksheetSqlMap
        self.spreadsheetId = spreadsheetId
        self.copyTo = copyTo
        self.parentId = parentId

    def execute(self, context):
        """
        Executes all the given sql statements and uploads them to their corresponding sheets.
        """
        spreadsheet_id = self.spreadsheetId
        if self.copyTo:
            hook = GoogleDriveHookV2()
            spreadsheet_id = hook.create_copy(
                fileId=self.spreadsheetId,
                copyTo=self.copyTo,
                parentId=self.parentId
            )

        for k in self.worksheetSqlMap.keys():
            self.redshiftToSheets(
                query=self.worksheetSqlMap[k],
                spreadsheetId=spreadsheet_id,
                worksheetName=k
            )

    def _updateWorksheetWithCsv(self, spreadsheetId, worksheetName, file):
        """
        Replaces all the entries in the worksheet in the given spreadsheet with the entries in
        the given csv.
        """
        hook = GoogleSheetsHook()
        worksheet_id = hook.get_worksheet_id(
            spreadsheetId=spreadsheetId, worksheetName=worksheetName)
        hook.upload_csv_to_worksheet(
            spreadsheetId=spreadsheetId, worksheetId=worksheet_id, file=file)

    def redshiftToSheets(self, query, spreadsheetId, worksheetName):
        """
        Updates a worksheet on a spreadsheet with the given redshift query.

        """

        pg = PostgresHook()
        copy_auth = RedshiftAuthorizationHook()

        with pg.get_conn() as connection:
            with connection.cursor() as cursor:
                key = "unload/sheets_{}_".format(str(uuid.uuid4()))

                s3 = S3Hook(aws_conn_id=connections.aws)

                cursor.execute(
                    """
                    UNLOAD (%s)
                    TO %s
                    CREDENTIALS %s
                    FORMAT AS CSV
                    HEADER
                    PARALLEL off;
                    """, (query,'s3://sheets/' + key,copy_auth)
                )
                self.log.info("Executed query: " + query)

                obj = s3.get_key(
                    bucket_name='sheets', key=key + "000")
                self._updateWorksheetWithCsv(
                    spreadsheetId, worksheetName, obj.get()['Body'])
                s3.delete_objects(
                    bucket='sheets', keys=key + "000")
                self.log.info("Deleted s3 object: " + key)

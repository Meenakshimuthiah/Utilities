//utility that reads from Dynatrace using API

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.logging.Logger

import com.google.common.io.ByteStreams
import org.apache.http.{Header, StatusLine}
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.{HttpGet, HttpUriRequest}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import spray.json._

import scala.collection.immutable.ArraySeq
import scala.collection.mutable.ArrayBuffer

private[dynatrace] class DynatraceUtils(config: EtlConfig) {

  private val log = Logger.getLogger(getClass.getName)

  def callDynatrace(apiName: String, filters: Map[String, String] = Map[String, String]()): IterableOnce[JsObject] = {

    val url = "https://" + hostName + "/api/" + apiName

    log.info(s"Calling $url with $filters")

    var objList: ArrayBuffer[JsObject] = new ArrayBuffer()
    log.info(s"Get page ${objList.size} from ${url}")
    var obj = httpCalls(url, filters.updated("Api-Token", token)).asJsObject
    objList += obj
    while (obj.fields.contains("nextPageKey") && obj.fields("nextPageKey") != JsNull) {
      val nextPageKey: JsValue = obj.fields("nextPageKey")
      log.info(s"Get page ${objList.size} from $url")
      obj = httpCalls(url, Map(("Api-Token", token), ("nextPageKey",
        nextPageKey.asInstanceOf[JsString].value))).asJsObject
      objList += obj

    }
    objList
  }

  private def httpCalls(baseUrl: String, filter: Map[String, String]): JsValue = {
    val queryParams = for ((k, v) <- filter) yield
      s"${URLEncoder.encode(k, StandardCharsets.UTF_8.name())}=${URLEncoder.encode(v, StandardCharsets.UTF_8.name())}"
    val url = baseUrl + (if (queryParams.nonEmpty) queryParams.mkString("?","&","") else "")
    log.info("Sleeping for 2400 milliseconds")
    Thread.sleep(2400)
    val client = HttpClientBuilder.create.build
    try {
      val (status, headers, body) = clientExecute(client, new HttpGet(url))
      if (status.getStatusCode < 200 || status.getStatusCode > 300) {
        val msg =
          s"""Error calling ${baseUrl}
             |$status
             |${headers.mkString("\n")}
             |${new String(body.filter(b => b >= 32 || b == '\n' || b == '\t'),"US-ASCII")}""".stripMargin
        throw new Exception(msg)
      }
      log.info(status.toString)
      val rateLimit = headers.filter(_.getName.toLowerCase.startsWith("x-ratelimit"))
        .map(o => o.getName.toLowerCase -> o.getValue).toMap
      log.info(s"rate limit headers: $rateLimit")
      val resetAt = rateLimit.get("x-ratelimit-reset").map{_.toLong / 1000L}
      log.info(s"reset at ${resetAt.map(Instant.ofEpochMilli)}")
      if (rateLimit.getOrElse("x-ratelimit-remaining", "0").toInt < 5) {
        log.info("Pausing due to low ratelimit-remaining value")
        val sleepFor = resetAt
          .map(_ - System.currentTimeMillis())
          .map(w => math.max(2400, math.min(60000, w)))
          .getOrElse(2400L)
        log.info(s"Sleeping for $sleepFor millis")
        Thread.sleep(sleepFor)
      }
      JsonParser(body)
    } finally {
      client.close()
    }
  }

  private def clientExecute(client: CloseableHttpClient, request: HttpUriRequest): (StatusLine, IndexedSeq[Header], Array[Byte]) = {
    val httpResponse = client.execute(request)
    try {
      val headers = ArraySeq.from(httpResponse.getAllHeaders())
      val status = httpResponse.getStatusLine
      val body = {
        val is = httpResponse.getEntity.getContent
        try {
          ByteStreams.toByteArray(is)
        } finally {
          is.close()
        }
      }
      (status, headers, body)
    } finally {
      httpResponse.close()
    }
  }

}

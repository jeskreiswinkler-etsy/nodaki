package com.etsy.scalding.jobs

import com.twitter.scalding.{Args, Job, JobTest, Tsv}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}
import JMET1Test._
import analytics.sequence.Visit
import com.etsy.scalding.VisitLog
import analytics.Event
import com.twitter.scalding._
import com.etsy.scalding._
import com.etsy.scalding.testing.{EtsySpecification, VisitLogMockTrait}


object JMET1Test {
  class RunJMET1(args: Args) extends AnalyticsJob(args) with JMET1Utils {
    val a = 0
    runJMET1(a).write(Tsv("output.tsv"))
  }
}

class JMET1Test extends EtsySpecification[JMET1] with Matchers with MockitoSugar {
  override lazy val TestDateString = "2015_02_17"

  val visitsDate = TestDateTstamp

  val cookies = "uaid=uaid%3DHku0JxMePI10mWv35K3mlbdeMOM8%26isaa%3D%26_now%3D1372714784%26_slt%3Dau8QHNTR%26_kid%3D1%26_ver%3D1%26_mac%3DqwKJ_ewJL0ptALwngKdlJriyyNLEsaOjy5Iv0BV_svI.; autosuggest_split=1; last_browse_page=%2F; __utma=111461200.1170468585.1322854689.1372714737.1373036127.649;"
  val user_agent = "user_agent => Mozilla/5.0 (iPad; CPU OS 7_1_1 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D201 Safari/9537.53"
  val ref0 = "url0"
  val ref1 = "url1"
  val ref2 = "url2"

  val botloc = "http://www.etsy.com/shop/Styloosh"


  val event1 = Event(eventType = "search",
    visitsDate, Event.map(
      ".loc" -> ref0,
      ".ref" -> ref0,
      ".event_source" -> "web",
      "ip" -> "69.119.232.171",
      "user_agent" -> user_agent,
      "primary_event" -> "true")
  )

  val event2 = Event(eventType = "search",
    visitsDate + 2000L,
    Event.map(
      ".loc" -> ref1,
      ".ref" -> ref1,
      ".event_source" -> "web",
      "ip" -> "69.119.232.171",
      "user_agent" -> user_agent,
      "primary_event" -> "true")
  )

  val event3 = Event(eventType = "backend_cart_payment",
    visitsDate + 4000L,
    Event.map(
      ".loc" -> ref2,
      ".ref" -> ref1,
      ".event_source" -> "web",
      "ip" -> "69.119.232.171",
      "user_agent" -> user_agent,
      "primary_event" -> "true")
  )

  val visits = Seq(
    Visit(List(event1, event2), "AAA"),
    Visit(List(event1, event3), "BBB"),
    Visit(List(event3, event1), "CCC"),
    Visit(List(event1, event2, event3), "DDD")).map(MockParquetVisitConverter.convertVisit)

  "runJMET1" should "get" + "basicCheck" in {
    JobTest(jobName)
      .arg("date", TestDateString)
      .source(VisitLog(), visits)
      .sink[Seq[Visit]](Tsv("output"))({ outBuf => {
          println(outBuf)
          outBuf(0) should be("DDD")
        }
      })
      .runHadoop
      .finish
  }
}
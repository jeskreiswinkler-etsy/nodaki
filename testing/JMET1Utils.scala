package com.etsy.scalding.jobs

import analytics.sequence.Visit
import cascading.pipe.Pipe
import com.etsy.scalding._
import com.twitter.scalding.{Job, Tsv, DateRange}


trait JMET1Utils extends AnalyticsJob {
  def runJMET1(a:Int) :Pipe = {
    val datewant = "2009-10-01"
      VisitLog().filter('visit) { visit: Visit =>
      visit.hasEventWithType("search") &&
        visit.hasEventWithType("backend_cart_payment")
      }
  }
}
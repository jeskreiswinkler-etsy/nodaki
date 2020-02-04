package com.etsy.scalding.jobs

import com.twitter.scalding.{Args, Job, Tsv}
import com.etsy.scalding._


// This file is boring. Go look at ExampleTestableJobUtils.scala!
class JMET1(args : Args) extends AnalyticsJob(args) with JMET1Utils {

  val a = 2
  runJMET1(a).write(Tsv("data/output.tsv", writeHeader = true))

}
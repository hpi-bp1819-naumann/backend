package com.amazon.deequ.backend.jobmanagement

import java.util.Date

case class ExecutableAnalyzerJob(analyzerFunc: () => Any) extends Runnable {

  var status: JobStatus.Value = JobStatus.ready
  var result: Any = None
  var runtimeInNs: Long = -1
  var startTime: Date = _
  var endTime: Date = _

  def run(): Unit = {
    status = JobStatus.running
    startTime = new Date()

    result = analyzerFunc()
    endTime = new Date()
    status = JobStatus.completed

    runtimeInNs = endTime.getTime - startTime.getTime
  }
}

object JobStatus extends Enumeration {
  val ready, running, completed = Value
}
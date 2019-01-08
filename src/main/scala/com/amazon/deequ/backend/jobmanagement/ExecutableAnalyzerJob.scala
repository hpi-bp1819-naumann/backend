package com.amazon.deequ.backend.jobmanagement

case class ExecutableAnalyzerJob(analyzerName: String, analyzerFunc: () => Any, parameters: Map[String, String]) extends Runnable {

  var status: JobStatus.Value = JobStatus.ready
  var result: Any = None
  var startTime: Long = _
  var endTime: Long = _

  def run(): Unit = {
    status = JobStatus.running
    startTime = System.currentTimeMillis()

    result = analyzerFunc()
    endTime = System.currentTimeMillis()
    status = JobStatus.completed
  }
}

object JobStatus extends Enumeration {
  val ready, running, completed = Value
}
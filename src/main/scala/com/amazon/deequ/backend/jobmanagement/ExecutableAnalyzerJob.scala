package com.amazon.deequ.backend.jobmanagement

case class ExecutableAnalyzerJob(analyzerName: String, analyzerFunc: () => Any, parameters: Map[String, String]) extends Runnable {

  var status: JobStatus.Value = JobStatus.ready
  var result: Any = None
  var startTime: Long = _
  var endTime: Long = _
  var errorMessage: Option[String] = None

  def run(): Unit = {
    status = JobStatus.running
    startTime = System.currentTimeMillis()

    try {
      result = analyzerFunc()
      status = JobStatus.completed
    }
    catch {
      case e: JobManagementRuntimeException =>
        status = JobStatus.error
        errorMessage = Some(e.getMessage)
    }

    endTime = System.currentTimeMillis()
  }
}

object JobStatus extends Enumeration {
  val ready, running, completed, error = Value
}
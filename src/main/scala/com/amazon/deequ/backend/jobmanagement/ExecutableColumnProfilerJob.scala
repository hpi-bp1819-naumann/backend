package com.amazon.deequ.backend.jobmanagement

/**
  *
  * @param tableName Name of table to execute analyzer on
  * @param jobFunction Function that returns a Map of an analyzer to its calculated metric
  */

case class ExecutableColumnProfilerJob (
                                         tableName: String,
                                         jobFunction: () => Map[String, Any])
  extends Runnable {

  var status: JobStatus.Value = JobStatus.ready
  var result: Map[String, Any] = Map()
  var startTime: Long = _
  var endTime: Long = _
  var errorMessage: Option[String] = None
  var thread: Thread = _

  def start(): Unit = {
    thread = new Thread(this)
    thread.start()
  }

  def cancel(): Unit = {
    thread.interrupt()
  }

  def run(): Unit = {
    status = JobStatus.running
    startTime = System.currentTimeMillis()

    try {
      result = jobFunction()
      status = JobStatus.completed
    }
    catch {
      case e: JobManagementRuntimeException =>
        status = JobStatus.error
        errorMessage = Some(e.getMessage)
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
    }

    endTime = System.currentTimeMillis()
  }
}
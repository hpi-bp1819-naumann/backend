package com.amazon.deequ.backend.jobmanagement

import com.amazon.deequ.metrics.Metric

/**
  *
  * @param jobName
  * @param tableName Name of table to execute analyzer on
  * @param context "spark" or "jdbc"
  * @param jobFunction Function that returns a Map of an analyzer to its calculated metric
  * @param analyzerToParam Map from an analyzer to its corresponding parameters
  */
case class ExecutableAnalyzerJob (
                                   jobName: String,
                                   tableName: String,
                                   context: String,
                                   jobFunction: () => Map[Any, Metric[_]],
                                   analyzerToParam: Map[Any, AnalyzerParams])

  extends Runnable {

  var status: JobStatus.Value = JobStatus.ready
  var result: Map[Any, Metric[_]] = Map()
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

object JobStatus extends Enumeration {
  val ready, running, completed, error = Value
}
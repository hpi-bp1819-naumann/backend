package com.amazon.deequ.backend.jobmanagement

case class ExecutableAnalyzerJob(analyzerFunc: () => Any, context: String) extends Runnable {

  var status: JobStatus.Value = JobStatus.ready
  var result: Any = None
  var runtimeInNs: Long = -1

  def run(): Unit = {
    status = JobStatus.running
    val startTime = System.nanoTime()

    result = analyzerFunc()
    val endTime = System.nanoTime()
    status = JobStatus.completed

    runtimeInNs = endTime - startTime
  }
}

object JobStatus extends Enumeration {
  val ready, running, completed = Value
}
package com.amazon.deequ.backend.servlets

import com.amazon.deequ.backend.jobmanagement.{JobManagement, JobStatus}
import org.json4s.JsonDSL._
import org.scalatra._

class JobManagementServlet extends Servlet {

  private val jobManager = new JobManagement

  get("/") {
    val jobs = jobManager.getJobs
    Ok("jobs" -> jobs)
  }

  delete("/") {
    try {
      val jobs = jobManager.getJobs
      for (j <- jobs) {
        val jobId = j("id").toString()
        if (j("status") == "finished"){
          jobManager.deleteJob(jobId)
        }
      }
      Ok("message" -> "Deleted all finished jobs")
    } catch errorHandling
  }

  get("/:jobId") {
    try {
      val jobId = params("jobId")
      val details = jobManager.getJob(jobId)
      Ok("job" -> details)
    } catch errorHandling
  }

  delete("/:jobId") {
    try {
      val jobId = params("jobId")
      jobManager.deleteJob(jobId)
      Ok("message" -> "Deleted job")
    } catch errorHandling
  }

  get("/analyzers") {
    try {
      val analyzers = jobManager.getAvailableAnalyzers
      Ok("analyzers" -> analyzers)
    } catch errorHandling
  }

  post("/:analyzer/start") {
    try {
      val analyzer = params("analyzer")
      val jobId = jobManager.startJob(analyzer, parsedBody)
      val response = ("message" -> "Successfully started job") ~
        ("analyzer" -> analyzer) ~ ("jobId" -> jobId.toString)
      Ok(response)
    } catch errorHandling
  }

  get("/:jobId/status") {
    try {
      val jobId = params("jobId")
      val status = jobManager.getJobStatus(jobId)
      var response = ("jobId" -> jobId) ~ ("status" -> status.toString)

      response = status match {
        case JobStatus.error => response ~ ("message" -> jobManager.getErrorMessage(jobId).get)
      }

      Ok(response)
    } catch errorHandling
  }

  get("/:jobId/params") {
    try {
      val jobId = params("jobId")
      val jobParams = jobManager.getJobParams(jobId)
      val response = ("jobId" -> jobId) ~ ("params" -> jobParams)
      Ok(response)
    } catch errorHandling
  }

  get("/:jobId/result") {
    try {
      val jobId = params("jobId")
      val result = jobManager.getJobResult(jobId)
      val response = ("jobId" -> jobId) ~ ("result" -> result.toString)
      Ok(response)
    } catch errorHandling
  }

  get("/:jobId/runtime") {
    try {
      val jobId = params("jobId")
      val runtime = jobManager.getJobRuntime(jobId)
      val response = ("jobId" -> jobId) ~ ("runtime" -> runtime.toString)
      Ok(response)
    } catch errorHandling
  }

}

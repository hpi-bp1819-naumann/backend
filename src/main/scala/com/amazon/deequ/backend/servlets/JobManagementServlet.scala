package com.amazon.deequ.backend.servlets

import com.amazon.deequ.backend.jobmanagement.JobManagement
import org.json4s.JsonDSL._
import org.json4s.{DefaultFormats, Formats}
import org.scalatra._
import org.scalatra.json._
import org.slf4j.LoggerFactory

class JobManagementServlet extends ScalatraServlet {

  private val logger =  LoggerFactory.getLogger(getClass)

  private val jobManager = new JobManagement

  get("/analyzers") {
    val analyzers = jobManager.getAvailableAnalyzers()
    compactRender("analyzers" -> analyzers)
  }

  post("/:analyzer/start") {
    logger.debug("start...")

    val jsonString = request.body
    logger.debug(jsonString)
    implicit val formats = DefaultFormats
    val jValue = parse(jsonString)

    val analyzer = params("analyzer")
    val jobId = jobManager.startJob(analyzer, jValue)
    val response = ("message" -> "Successfully started job") ~
      ("analyzer" -> analyzer) ~ ("jobId" -> jobId.toString)
    compactRender(response)
  }

  get("/:jobId/status") {
    val jobId = params("jobId")
    val status = jobManager.getJobStatus(jobId)
    val response = ("jobId" -> jobId) ~ ("status" -> status.toString)
    compactRender(response)
  }

  get("/:jobId/result") {
    val jobId = params("jobId")
    val result = jobManager.getJobResult(jobId)
    val response = ("jobId" -> jobId) ~ ("result" -> result.toString)
    compactRender(response)
  }

  get("/:jobId/runtime") {
    val jobId = params("jobId")
    val runtime = jobManager.getJobRuntime(jobId)
    val response = ("jobId" -> jobId) ~ ("runtime" -> runtime.toString)
    compactRender(response)
  }
}

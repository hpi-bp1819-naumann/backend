package com.amazon.deequ.backend.servlets

import com.amazon.deequ.backend.jobmanagement.JobManagement
import org.json4s.JsonDSL._
import org.json4s.{DefaultFormats, Formats}
import org.scalatra._
import org.scalatra.json._
import org.slf4j.LoggerFactory

class JobManagementServlet extends ScalatraServlet
  with JacksonJsonSupport {
  protected implicit lazy val jsonFormats: Formats = DefaultFormats

  private val logger =  LoggerFactory.getLogger(getClass)

  private val jobManager = new JobManagement


  def generateErrorResponse(ex: Exception): ActionResult = {
    val response = ("message" -> "There was an error during processing your request") ~
      ("error" -> ("type" -> ex.getClass.toString) ~ ("message" -> ex.getMessage))
    BadRequest(response)
  }

  val errorHandling: PartialFunction[Throwable, ActionResult] = {
    case e: Exception => generateErrorResponse(e)
  }

  before() {
    contentType = formats("json")
  }

  get("/analyzers") {
    try {
      val analyzers = jobManager.getAvailableAnalyzers()
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
      val response = ("jobId" -> jobId) ~ ("status" -> status.toString)
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

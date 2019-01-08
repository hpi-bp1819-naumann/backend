package com.amazon.deequ.backend.jobmanagement

import java.util.UUID.randomUUID

import com.amazon.deequ.backend.jobmanagement.analyzerJobs._
import org.json4s.JValue

import scala.collection.immutable.ListMap

class JobManagement {

  private var jobs = Map[String, ExecutableAnalyzerJob]()

  private val availableAnalyzers = ListMap[String, AnalyzerJob[_]](
    "completeness" -> CompletenessAnalyzerJob,
    "compliance" -> ComplianceAnalyzerJob,
    "correlation" -> CorrelationAnalyzerJob,
    "countDistinct" -> CountDistinctAnalyzerJob,
    "dataType" -> DataTypeAnalyzerJob,
    "distinctness" -> DistinctnessAnalyzerJob,
    "entropy" -> EntropyAnalyzerJob,
    "histogram" -> HistogramAnalyzerJob,
    "maximum" -> MaximumAnalyzerJob,
    "mean" -> MeanAnalyzerJob,
    "minimum" -> MinimumAnalyzerJob,
    "patternMatch" -> PatternMatchAnalyzerJob,
    "size" -> SizeAnalyzerJob,
    "standardDeviation" -> StandardDeviationAnalyzerJob,
    "sum" -> SumAnalyzerJob,
    "uniqueness" -> UniquenessAnalyzerJob,
    "uniqueValueRatio" -> UniqueValueRatioAnalyzerJob
  )

  def getAvailableAnalyzers: Seq[Map[String, Any]] = {
    availableAnalyzers.map(
      entry => Map[String, Any](
        "name" -> entry._2.name, "key" -> entry._1, "description" -> entry._2.description,
      "parameters" -> entry._2.acceptedRequestParams().map(param => Map[String, Any](
        "name" -> param.name,
        "type" -> param._type
    )))).toSeq
  }

  def getJob(jobId: String): Map[String, Any] = {
    val job = Option(jobs(jobId))
    job match {
      case Some(theJob) =>
        Map[String, Any]("id" -> jobId,
          "status" -> theJob.status.toString,
          "startingTime" -> theJob.startTime,
          "finishingTime" -> theJob.endTime,
          "result" -> theJob.result,
          "name" -> theJob.analyzerName,
          "params" -> theJob.parameters
      )
      case None =>
        throw new IllegalArgumentException("Job Id is not assigned")
    }
  }

  def getJobs: Seq[Map[String, Any]] = {
    jobs.map {
      case (id: String, job: ExecutableAnalyzerJob) =>
        val status = job.status
        var m = Map[String, Any](
          "id" -> id,
          "name" -> job.analyzerName,
          "status" -> status.toString)
        m += "startingTime" -> job.startTime
        if (status == JobStatus.completed) {
          m += ("result" -> job.result)
          m += ("finishingTime" -> job.endTime)
        }
        m
    }.toSeq
  }

  def startJob(requestedAnalyzer: String, params: JValue): String = {
    val jobId = randomUUID().toString.replace("-", "")

    if (!availableAnalyzers.exists(_._1 == requestedAnalyzer)) {
      throw new NoSuchAnalyzerException(
        s"There is no analyzer called $requestedAnalyzer. " +
          s"Available analyzers are ${availableAnalyzers.keys.mkString("[", ", ", "]")}")
    }

    val analyzer = availableAnalyzers(requestedAnalyzer)

    jobs += (jobId -> analyzer.from(params))
    new Thread(jobs(jobId)).start()

    jobId
  }

  def deleteJob(jobId: String): Unit = {
    jobs -= jobId
  }

  def getJobStatus(jobId: String): JobStatus.Value = {
    jobs(jobId).status
  }

  def getJobParams(jobId: String): Map[String, String] = {
    jobs(jobId).parameters
  }

  def getJobResult(jobId: String): Any = {
    jobs(jobId).result
  }

  def getJobRuntime(jobId: String): Any = {
    val job = jobs(jobId)
    job.endTime - job.startTime
  }
}

trait AnalyzerParams {
  val context: String
  val table: String
}

case class ColumnAnalyzerParams(context: String, table: String, var column: String)
  extends AnalyzerParams

case class ColumnAndWhereAnalyzerParams(context: String, table: String,
                                        var column: String,
                                        var where: Option[String] = None)
  extends AnalyzerParams

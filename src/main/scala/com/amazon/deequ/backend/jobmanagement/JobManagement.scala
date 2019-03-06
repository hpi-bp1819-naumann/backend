package com.amazon.deequ.backend.jobmanagement

import java.util.UUID.randomUUID

import com.amazon.deequ.backend.jobmanagement.analyzerJobs._
import com.amazon.deequ.backend.jobmanagement.extractors.{DistinctnessAnalyzerExtractor, UniquenessAnalyzerExtractor}
import org.json4s.JValue

import scala.collection.immutable.ListMap

class JobManagement {
  private var jobs = synchronized(Map[String, ExecutableAnalyzerJob]())

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
        val analyzerResponses = theJob.result.map { case (analyzer, metric) =>
          val params = theJob.analyzerToParam(analyzer)
          val analyzerKey = params.analyzer

          var analyzerResponse = Map[String, Any](
            "result" -> metric.value,
            "params" -> params.toMap
          )

          if (theJob.context == AnalyzerContext.jdbc) {
            analyzerKey match {
              case "uniqueness" | "uniqueValueRatio" =>
                analyzerResponse += ("query" -> UniquenessAnalyzerExtractor.parseQuery(theJob.tableName,
                  params.asInstanceOf[MultiColumnAnalyzerParams]))
              case "distinctness" | "countDistinct"  =>
                analyzerResponse += ("query" -> DistinctnessAnalyzerExtractor.parseQuery(theJob.tableName,
                  params.asInstanceOf[MultiColumnAnalyzerParams]))
              case _ =>
            }
          }

          analyzerResponse
        }

        Map[String, Any]("id" -> jobId,
          "status" -> theJob.status.toString,
          "startingTime" -> theJob.startTime,
          "finishingTime" -> theJob.endTime,
          "errorMessage" -> theJob.errorMessage,
          "name" -> theJob.jobName,
          "analyzers" -> analyzerResponses
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
          "name" -> job.jobName,
          "status" -> status.toString)
        m += "startingTime" -> job.startTime
        if (status == JobStatus.completed) {
          m += ("result" -> job.result)
          m += ("finishingTime" -> job.endTime)
        } else if (status == JobStatus.error) {
          m += ("errorMessage" -> job.errorMessage)
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

    val job = analyzer.from(params)
    jobs += (jobId -> job)
    job.start()

    jobId
  }

  def startJobs(tableName: String, context: String, parsedBody: JValue): Any = {
    if (!AnalyzerContext.availableContexts().contains(context)) {
      throw new NoSuchContextException(
        s"There is not supported context called $context. " +
          s"Available contexts are ${AnalyzerContext.availableContexts().mkString("[", ", ", "]")}")
    }

    val analysisRunJob = AnalysisRun(tableName, context).from(parsedBody)
    val jobId = randomUUID().toString.replace("-", "")
    jobs += (jobId -> analysisRunJob)
    analysisRunJob.start()

    jobId

  }

  def deleteJob(jobId: String): Unit = {
    jobs -= jobId
  }

  def cancelJob(jobId: String): Unit = {
    val job = Option(jobs(jobId))
    job match {
      case Some(theJob) =>
        theJob.status match {
          case JobStatus.running =>
            theJob.cancel()
            deleteJob(jobId)
          case _ =>
            throw new IllegalArgumentException("The job is not running and can therefore not be canceled")
        }
      case None =>
        throw new IllegalArgumentException("Job Id is not assigned")
    }
  }

  def getJobStatus(jobId: String): JobStatus.Value = {
    jobs(jobId).status
  }

  def getErrorMessage(jobId: String): Option[String] = {
    jobs(jobId).errorMessage
  }

  def getJobParams(jobId: String): Map[String, Any] = {
    // TODO: rework
    Map()
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
  val analyzer: String
  def toMap: Map[String, Any] = {
    Map("analyzer" -> analyzer)
  }
}

case class ColumnAnalyzerParams(analyzer:String, column: String)
  extends AnalyzerParams {
  override def toMap: Map[String, Any] = {
    super.toMap ++ Map("column" -> column)
  }
}

case class ColumnAndWhereAnalyzerParams(analyzer:String,
                                        column: String,
                                        where: Option[String] = None)
  extends AnalyzerParams {
  override def toMap: Map[String, Any] = {
    super.toMap ++ Map("column" -> column, "where" -> where)
  }
}

case class MultiColumnAnalyzerParams(analyzer:String,
                                     columns: Seq[String])
  extends AnalyzerParams {
  override def toMap: Map[String, Any] = {
    super.toMap ++ Map("columns" -> columns)
  }
}

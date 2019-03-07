package com.amazon.deequ.backend.jobmanagement

import java.util.UUID.randomUUID

import com.amazon.deequ.backend.jobmanagement.extractors.{AnalyzerExtractor, DistinctnessAnalyzerExtractor, UniquenessAnalyzerExtractor}
import com.amazon.deequ.metrics.Distribution
import org.json4s.JValue

class JobManagement {
  private var jobs = synchronized(Map[String, ExecutableAnalyzerJob]())

  def getAvailableAnalyzers: Seq[Map[String, Any]] = {
    AnalysisRun.availableExtractors.map {
      case (analyzerKey: String, extractor: AnalyzerExtractor[_]) =>
        val allowedParameters =  extractor.acceptedRequestParams().map(
          param => Map[String, Any]("name" -> param.name, "type" -> param._type))
        Map[String, Any](
          "name" -> extractor.name,
          "key" -> analyzerKey,
          "description" -> extractor.description,
          "parameters" -> allowedParameters
          )
    }.toSeq
  }

  def getJob(jobId: String): Map[String, Any] = {
    val job = Option(jobs(jobId))
    job match {
      case Some(theJob) =>
        convertJob(jobId, theJob)
      case None =>
        throw new IllegalArgumentException("Job Id is not assigned")
    }
  }

  def getJobs: Seq[Map[String, Any]] = {
    jobs.map {
      case (id: String, job: ExecutableAnalyzerJob) =>
        convertJob(id, job)
    }.toSeq
  }

  private def convertJob(jobId: String, job: ExecutableAnalyzerJob): Map[String, Any] = {
    val analyzerResponses = job.result.map { case (analyzer, metric) =>
      val params = job.analyzerToParam(analyzer)
      val analyzerKey = params.analyzer
      val analyzerName = AnalysisRun.availableExtractors(analyzerKey).name
      val result = metric.value

      var analyzerResponse = Map[String, Any](
        "name" -> analyzerName,
        "params" -> params.toMap
      )

      if (result.isSuccess) {
        analyzerResponse += "status" -> JobStatus.completed.toString
        val resultValue = result.get match {
          case distribution: Distribution => distribution.values
          case value => value
        }
        analyzerResponse += "result" -> resultValue

        if (job.context == AnalyzerContext.jdbc) {
          analyzerKey match {
            case "uniqueness" | "uniqueValueRatio" =>
              analyzerResponse += ("query" -> UniquenessAnalyzerExtractor.parseQuery(job.tableName,
                params.asInstanceOf[MultiColumnAnalyzerParams]))
            case "distinctness" | "countDistinct"  =>
              analyzerResponse += ("query" -> DistinctnessAnalyzerExtractor.parseQuery(job.tableName,
                params.asInstanceOf[MultiColumnAnalyzerParams]))
            case _ =>
          }
        }
      } else {
        analyzerResponse += "status" -> JobStatus.error.toString
        analyzerResponse += "errorMessage" -> result.toString
      }

      analyzerResponse
    }

    Map[String, Any](
      "id" -> jobId,
      "status" -> job.status.toString,
      "startingTime" -> job.startTime,
      "finishingTime" -> job.endTime,
      "errorMessage" -> job.errorMessage,
      "name" -> job.jobName,
      "table" -> job.tableName,
      "context" -> job.context,
      "analyzers" -> analyzerResponses
    )
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
    // TODO: rework
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

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

  def getAvailableAnalyzers(): Seq[Map[String, String]] = {
    availableAnalyzers.map(
      entry => Map[String, String](
        "name" -> entry._2.name, "key" -> entry._1, "description" -> entry._2.description)).toSeq
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

  def getJobStatus(jobId: String): JobStatus.Value = {
    jobs(jobId).status
  }

  def getJobResult(jobId: String): Any = {
    jobs(jobId).result
  }

  def getJobRuntime(jobId: String): Any = {
    jobs(jobId).runtimeInNs
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

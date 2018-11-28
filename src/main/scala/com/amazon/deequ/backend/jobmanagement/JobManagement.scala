package com.amazon.deequ.backend.jobmanagement

import net.liftweb.json._
import java.util.UUID.randomUUID
import com.amazon.deequ.backend.jobmanagement.analyzerJobs._

class JobManagement {

  private var jobs = Map[String, ExecutableAnalyzerJob]()

  private val availableAnalyzers = Map[String, AnalyzerJob](
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

  def getAvailableAnalyzers(): Set[String] = {
    availableAnalyzers.keySet
  }

  def startJob(requestedAnalyzer: String, params: JValue): String = {
    val jobId = randomUUID().toString.replace("-", "")

    if (!availableAnalyzers.exists(_._1 == requestedAnalyzer)) {
      throw new Exception("there is no analyzer called " + requestedAnalyzer)
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

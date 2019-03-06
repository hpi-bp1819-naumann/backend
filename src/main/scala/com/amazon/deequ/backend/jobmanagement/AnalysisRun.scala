package com.amazon.deequ.backend.jobmanagement

import com.amazon.deequ.analyzers.Analyzer
import com.amazon.deequ.analyzers.jdbc.{JdbcAnalyzer, Table}
import com.amazon.deequ.analyzers.runners.{AnalysisRunner, JdbcAnalysisRunner}
import com.amazon.deequ.backend.DbSettings.DbSettings
import com.amazon.deequ.backend.jobmanagement.extractors._
import com.amazon.deequ.backend.utils.JdbcUtils.{connectionProperties, withJdbc, withSpark}
import com.amazon.deequ.metrics.Metric
import org.json4s.JsonAST.JValue
import org.json4s.{DefaultFormats, Formats}

import scala.collection.immutable.ListMap

case class AnalysisRun(tableName: String, context: String) {
  implicit val formats: Formats = DefaultFormats

  def from(parsedBody: JValue): ExecutableAnalyzerJob = {

    val body = parsedBody.extract[Map[String, Seq[JValue]]]
    val requestedAnalyzers = body("analyzers")

    var params = Map[Any, AnalyzerParams]()

    val analysisRun = context match {
      case AnalyzerContext.jdbc =>
        val analyzerToParams = parseJdbcAnalyzers(requestedAnalyzers)
        params = analyzerToParams
        val analyzers = analyzerToParams.keys.map(analyzer => analyzer.asInstanceOf[JdbcAnalyzer[_, Metric[_]]]).toSeq

        () => withJdbc[Map[Any, Metric[_]]] { connection =>
          val table = Table(tableName, connection)
          JdbcAnalysisRunner.doAnalysisRun(table, analyzers).metricMap.asInstanceOf[Map[Any, Metric[_]]]
        }
      case AnalyzerContext.spark =>
        val analyzerToParams = parseSparkAnalyzers(requestedAnalyzers)
        params = analyzerToParams
        val analyzers = analyzerToParams.keys.map(analyzer => analyzer.asInstanceOf[Analyzer[_, Metric[_]]]).toSeq

        () => withSpark[Map[Any, Metric[_]]] { session =>
          val data = session.read.jdbc(DbSettings.dburi, tableName, connectionProperties())
          AnalysisRunner.doAnalysisRun(data, analyzers).metricMap.asInstanceOf[Map[Any, Metric[_]]]
        }
    }

    ExecutableAnalyzerJob("AnalysisRun", tableName, context, analysisRun, params)
  }

  def parseAnalyzerExtractors(requestedAnalyzers: Seq[JValue]): Seq[AnalyzerExtractor[_]] = {
    requestedAnalyzers.map(analyzerParams => {
      val extractedParams = analyzerParams.extract[Map[String, Any]]
      val analyzerName = extractedParams("analyzer").toString

      if (!AnalysisRun.availableExtractors.exists(_._1 == analyzerName)) {
        throw new NoSuchAnalyzerException(
          s"There is no analyzer called $analyzerName. " +
            s"Available analyzers are ${AnalysisRun.availableExtractors.keys.mkString("[", ", ", "]")}")
      }

      val analyzerExtractor = AnalysisRun.availableExtractors(analyzerName)

      try {
        analyzerExtractor.extractFromJson(analyzerParams)
      } catch {
        case e: org.json4s.MappingException =>
          throw new RequestParamsException(
            s"There seems to be an error in your request parameters. " +
              s"The parameter extraction failed with: " + e.msg)
      }

      analyzerExtractor
    })
  }

  def parseJdbcAnalyzers(requestedAnalyzers: Seq[JValue]): Map[Any, AnalyzerParams] = {
    val extractors = parseAnalyzerExtractors(requestedAnalyzers)

    extractors.map { extractor =>
      extractor.analyzerWithJdbc().asInstanceOf[Any] -> extractor.params.asInstanceOf[AnalyzerParams]
    }.toMap
  }

  def parseSparkAnalyzers(requestedAnalyzers: Seq[JValue]): Map[Any, AnalyzerParams] = {
    val extractors = parseAnalyzerExtractors(requestedAnalyzers)
    extractors.map(extractor =>
      extractor.analyzerWithSpark().asInstanceOf[Any] -> extractor.params.asInstanceOf[AnalyzerParams]
    ).toMap
  }
}

object AnalysisRun {
  val availableExtractors: ListMap[String, AnalyzerExtractor[_]] = ListMap[String, AnalyzerExtractor[_]](
    "completeness" -> CompletenessAnalyzerExtractor,
    "compliance" -> ComplianceAnalyzerExtractor,
    "correlation" -> CorrelationAnalyzerExtractor,
    "countDistinct" -> CountDistinctAnalyzerExtractor,
    "dataType" -> DataTypeAnalyzerExtractor,
    "distinctness" -> DistinctnessAnalyzerExtractor,
    "entropy" -> EntropyAnalyzerExtractor,
    "histogram" -> HistogramAnalyzerExtractor,
    "maximum" -> MaximumAnalyzerExtractor,
    "mean" -> MeanAnalyzerExtractor,
    "minimum" -> MinimumAnalyzerExtractor,
    "patternMatch" -> PatternMatchAnalyzerExtractor,
    "size" -> SizeAnalyzerExtractor,
    "standardDeviation" -> StandardDeviationAnalyzerExtractor,
    "sum" -> SumAnalyzerExtractor,
    "uniqueness" -> UniquenessAnalyzerExtractor,
    "uniqueValueRatio" -> UniqueValueRatioAnalyzerExtractor
  )
}

case class RequestParameter(name: String, _type: String)

object AnalyzerContext extends Enumeration {
  val jdbc = "jdbc"
  val spark = "spark"

  def availableContexts(): Seq[String] = {
    Seq(jdbc, spark)
  }
}

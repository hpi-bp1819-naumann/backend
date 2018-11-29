package com.amazon.deequ.backend.jobmanagement

import com.amazon.deequ.analyzers.jdbc.{JdbcAnalyzer, Table}
import com.amazon.deequ.analyzers.{Analyzer, State}
import com.amazon.deequ.backend.utils.JdbcUtils.{connectionProperties, jdbcUrl, withJdbc, withSpark}
import com.amazon.deequ.metrics.Metric
import org.json4s.{DefaultFormats, Formats, JValue}

abstract class AnalyzerJob[T <: AnalyzerParams] {

  val name: String
  val description: String

  implicit val formats: Formats = DefaultFormats

  def funcWithJdbc(params: T): Any
  def funcWithSpark(params: T): Any
  def extractFromJson(requestParams: JValue): T

  def from(requestParams: JValue): ExecutableAnalyzerJob = {
    val params = extractFromJson(requestParams)

    val func = () => params.context match {
      case AnalyzerContext.jdbc => funcWithJdbc(params)
      case AnalyzerContext.spark => funcWithSpark(params)

      case _ => throw new NoSuchContextException(
        s"There is not supported context called ${params.context}. " +
          s"Available contexts are ${AnalyzerContext.availableContexts().mkString("[", ", ", "]")}")
    }

    ExecutableAnalyzerJob(func)
  }

  def analyzerWithJdbc[S <: State[_], M <: Metric[_], A <: JdbcAnalyzer[S, M]](analyzer: A, tableName: String): Any = {
    withJdbc { connection =>
      val table = Table(tableName, connection)
      return analyzer.calculate(table).value
    }
  }

  def analyzerWithSpark[S <: State[_], M <: Metric[_], A <: Analyzer[S, M]](analyzer: A, tableName: String): Any = {
    withSpark { session =>
      val data = session.read.jdbc(jdbcUrl, tableName, connectionProperties())
      return analyzer.calculate(data).value
    }
  }
}

object AnalyzerContext extends Enumeration {
  val jdbc = "jdbc"
  val spark = "spark"

  def availableContexts(): Seq[String] = {
    Seq(jdbc, spark)
  }
}

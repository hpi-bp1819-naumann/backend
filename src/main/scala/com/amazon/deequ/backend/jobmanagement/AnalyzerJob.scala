package com.amazon.deequ.backend.jobmanagement

import com.amazon.deequ.analyzers.jdbc.{JdbcAnalyzer, Table}
import com.amazon.deequ.analyzers.{Analyzer, State}
import com.amazon.deequ.backend.utils.JdbcUtils.{connectionProperties, jdbcUrl, withJdbc, withSpark}
import com.amazon.deequ.metrics.Metric
import org.json4s.{DefaultFormats, Formats, JValue}

import scala.util.Failure


abstract class AnalyzerJob[T <: AnalyzerParams] {

  val name: String
  val description: String

  implicit val formats: Formats = DefaultFormats

  def funcWithJdbc(params: T): Any
  def funcWithSpark(params: T): Any
  def extractFromJson(requestParams: JValue): T
  val acceptedRequestParams: () => Array[RequestParameter]

  def extractFieldNames[M <: Product:Manifest]: Array[RequestParameter] = {
    implicitly[Manifest[M]].erasure.getDeclaredFields.map(param =>
      RequestParameter(param.getName, param.getType.getSimpleName))
  }

  def from(requestParams: JValue): ExecutableAnalyzerJob = {
    var params: Option[T] = None

    val map = requestParams.extract[Map[String, Any]]

    try {
      val extractedParams = extractFromJson(requestParams)
      params = Some(extractedParams)
    } catch {
      case e: org.json4s.MappingException =>
        throw new RequestParamsException(
          s"There seems to be an error in your request parameters. " +
          s"The parameter extraction failed with: " + e.msg)
    }

    if (!AnalyzerContext.availableContexts().contains(params.get.context)) {
      throw new NoSuchContextException(
        s"There is not supported context called ${params.get.context}. " +
          s"Available contexts are ${AnalyzerContext.availableContexts().mkString("[", ", ", "]")}")
    }

    val func = () => params.get.context match {
      case AnalyzerContext.jdbc => funcWithJdbc(params.get)
      case AnalyzerContext.spark => funcWithSpark(params.get)
    }

    ExecutableAnalyzerJob(name, func, map)
  }

  def analyzerWithJdbc[S <: State[_], M <: Metric[_], A <: JdbcAnalyzer[S, M]](analyzer: A, tableName: String): Any = {
    withJdbc { connection =>
      val table = Table(tableName, connection)
      val calc = analyzer.calculate(table)
      return calc.value.get
    }
  }

  def analyzerWithSpark[S <: State[_], M <: Metric[_], A <: Analyzer[S, M]](analyzer: A, tableName: String): Any = {
    withSpark { session =>
      val data = session.read.jdbc(jdbcUrl, tableName, connectionProperties())
      return analyzer.calculate(data).value
    }
  }
}

case class RequestParameter(name: String, _type: String)

object AnalyzerContext extends Enumeration {
  val jdbc = "jdbc"
  val spark = "spark"

  def availableContexts(): Seq[String] = {
    Seq(jdbc, spark)
  }
}

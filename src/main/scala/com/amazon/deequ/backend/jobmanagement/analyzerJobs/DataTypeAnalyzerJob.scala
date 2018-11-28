package com.amazon.deequ.backend.jobmanagement.analyzerJobs

import com.amazon.deequ.analyzers.jdbc._
import com.amazon.deequ.analyzers.{DataType, DataTypeHistogram}
import com.amazon.deequ.backend.jobmanagement.{AnalyzerJob, ExecutableAnalyzerJob}
import com.amazon.deequ.metrics.HistogramMetric
import net.liftweb.json.DefaultFormats
import net.liftweb.json.JsonAST.JValue

class DataTypeAnalyzerParams(var context: String, var table: String,
                             var column: String, var where: Option[String] = None)

object DataTypeAnalyzerJob extends AnalyzerJob {

  def from(requestParams: JValue): ExecutableAnalyzerJob = {
    implicit val formats = DefaultFormats
    val params = requestParams.extract[DataTypeAnalyzerParams]

    val func = () => params.context match {
      case "jdbc" => analyzerWithJdbc[DataTypeHistogram, HistogramMetric, JdbcDataType](
        JdbcDataType(params.column), params.table)
      case "spark" => analyzerWithSpark[DataTypeHistogram, HistogramMetric, DataType](
        DataType(params.column), params.table)

      case _ => throw new Exception("does not support context " + params.context)
    }

    ExecutableAnalyzerJob(func)
  }
}

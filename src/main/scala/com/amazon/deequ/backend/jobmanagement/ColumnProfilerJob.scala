package com.amazon.deequ.backend.jobmanagement

import com.amazon.deequ.profiles.{ColumnProfile, ColumnProfilerRunner, ColumnProfiles, NumericColumnProfile}
import com.amazon.deequ.analyzers.{MaxState, Maximum}
import com.amazon.deequ.metrics.DoubleMetric
import org.json4s.JValue
import com.amazon.deequ.backend.utils.JdbcUtils.{connectionProperties, jdbcUrl, withJdbc, withSpark}
import com.google.gson.{JsonArray, JsonObject, JsonPrimitive}

object ColumnProfilerJob extends AnalyzerJob[BaseParams] {

  val name = "ColumnProfiler"
  val description = "Returns some basic metrics for all columns of the given table"

  val acceptedRequestParams: () => Array[RequestParameter] =
    () => extractFieldNames[BaseParams]

  def extractFromJson(requestParams: JValue): BaseParams = {
    requestParams.extract[BaseParams]
  }

  def funcWithJdbc(params: BaseParams): Any = {
    val result = "test"
    throw new IllegalArgumentException("The column profiler job can only be executed via spark")
  }

  def toJsonObject(result: com.amazon.deequ.profiles.ColumnProfiles): Any = {
//    result.profiles.map {
//      case (name: String, profile: ColumnProfile) =>
//        "column" -> profile.column,
//        "completeness" -> profile.completeness,
//        "approximateNumDistinctValues" -> profile.approximateNumDistinctValues
//      )
//    }.toSeq

//      jobs.map {
//        case (id: String, job: ExecutableAnalyzerJob) =>
//          val status = job.status
//          var m = Map[String, Any](
//            "id" -> id,
//            "name" -> job.analyzerName,
//            "status" -> status.toString)
//          m += "startingTime" -> job.startTime
//          if (status == JobStatus.completed) {
//            m += ("result" -> job.result)
//            m += ("finishingTime" -> job.endTime)
//          }
//          m
//      }.toSeq
//    )
  }

  def funcWithSpark(params: BaseParams): Any = {

    withSpark { session =>
      val rows = session.read.jdbc(jdbcUrl, params.table, connectionProperties())
      val result = ColumnProfilerRunner().onData(rows).run()
      //return toJsonObject(ColumnProfiles.toJsonObject(result.profiles.values.toSeq))
      //return ColumnProfiles.toJson(result.profiles.values)
      return toJsonObject(result)
      //return result
    }
  }
}

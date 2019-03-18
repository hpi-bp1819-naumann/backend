package com.amazon.deequ.backend.jobmanagement

import com.amazon.deequ.profiles.{ColumnProfile, ColumnProfilerRunner, NumericColumnProfile}
import org.json4s.JValue
import com.amazon.deequ.backend.utils.JdbcUtils.{connectionProperties, jdbcUrl, withSpark}

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
    var json = Map[String, Any]()

    var columns = Set[Any]()

    result.profiles.values.foreach { profile =>

      var columnProfileJson = Map[String, Any]()
      columnProfileJson += ("column" -> profile.column)
      columnProfileJson += ("dataType" -> profile.dataType.toString)
      columnProfileJson += ("isDataTypeInferred" -> profile.isDataTypeInferred.toString)

      if (profile.typeCounts.nonEmpty) {
        var typeCountsJson = Map[String, Any]()
        profile.typeCounts.foreach { case (typeName, count) =>
          typeCountsJson += (typeName -> count.toString)
        }
      }

      columnProfileJson += ("completeness" -> profile.completeness)
      columnProfileJson += ("approximateNumDistinctValues" ->
        profile.approximateNumDistinctValues)

      if (profile.histogram.isDefined) {
        val histogram = profile.histogram.get
        var histogramJson = Set[Any]()

        histogram.values.foreach { case (name, distributionValue) =>
          var histogramEntry = Map[String, Any]()
          histogramEntry += ("value" -> name)
          histogramEntry +=("count"-> distributionValue.absolute)
          histogramEntry += ("ratio"-> distributionValue.ratio)
          histogramJson += histogramEntry
        }

        columnProfileJson += ("histogram" -> histogramJson)
      }

      profile match {
        case numericColumnProfile: NumericColumnProfile =>
          numericColumnProfile.mean.foreach { mean =>
            columnProfileJson += ("mean"-> mean)
          }
          numericColumnProfile.maximum.foreach { maximum =>
            columnProfileJson += ("maximum"-> maximum)
          }
          numericColumnProfile.minimum.foreach { minimum =>
            columnProfileJson += ("minimum"-> minimum)
          }
          numericColumnProfile.sum.foreach { sum =>
            columnProfileJson += ("sum"-> sum)
          }
          numericColumnProfile.stdDev.foreach { stdDev =>
            columnProfileJson += ("stdDev"-> stdDev)
          }
        case _ =>
      }

      columns += columnProfileJson
    }

    json += ("columns"-> columns)
    json
  }

  def funcWithSpark(params: BaseParams): Any = {

    withSpark { session =>
      val rows = session.read.jdbc(jdbcUrl, params.table, connectionProperties())
      val result = ColumnProfilerRunner().onData(rows).run()
      return toJsonObject(result)
    }
  }
}

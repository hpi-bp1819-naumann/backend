package com.amazon.deequ.backend.jobmanagement

import com.amazon.deequ.backend.DbSettings.DbSettings
import com.amazon.deequ.backend.jobmanagement
import com.amazon.deequ.backend.utils.JdbcUtils.{connectionProperties, withSpark}
import com.amazon.deequ.profiles.{ColumnProfilerRunner, NumericColumnProfile}

case class ColumnProfilerJob(tableName: String){

  def profile: ExecutableColumnProfilerJob = {
    val analysisRun =
        () => withSpark[Map[String, Any]] { session =>
          val data = session.read.jdbc(DbSettings.dburi, tableName, connectionProperties())
          toJsonObject(ColumnProfilerRunner().onData(data).run())
    }
    jobmanagement.ExecutableColumnProfilerJob(tableName, analysisRun)
  }

  def toJsonObject(result: com.amazon.deequ.profiles.ColumnProfiles): Map[String, Any] = {
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
}

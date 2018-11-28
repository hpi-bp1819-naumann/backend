package com.amazon.deequ.backend.jobmanagement

import com.amazon.deequ.analyzers.{Analyzer, MaxState, Size, State}
import com.amazon.deequ.analyzers.jdbc.{JdbcAnalyzer, Table}
import com.amazon.deequ.metrics.{DoubleMetric, Metric}
import com.amazon.deequ.backend.utils.JdbcUtils.{connectionProperties, jdbcUrl, withJdbc, withSpark}
import net.liftweb.json._

trait AnalyzerJob {
  def from(request: JValue): ExecutableAnalyzerJob

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

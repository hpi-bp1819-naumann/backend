package com.amazon.deequ.backend.utils

import java.net.ConnectException
import java.sql.{Connection, DriverManager, SQLException}
import java.util.Properties

import com.amazon.deequ.backend.DbSettings.DbSettings
import com.amazon.deequ.backend.jobmanagement.{AnalyzerRuntimeException, SQLConnectionException}
import org.apache.spark.sql.SparkSession

import scala.io.Source

object JdbcUtils {

  val jdbcUrl = DbSettings.dburi

  def connectionProperties(): Properties = {
    DbSettings.connectionProperties
  }

  def withJdbc[T](func: Connection => T): T = {
    var connection: Option[Connection] = None

    try {
      connection = Some(DriverManager.getConnection(jdbcUrl, connectionProperties()))
      func(connection.get)
    } catch {
      case e: ConnectException =>
        throw new SQLConnectionException(
          s"Could not establish a connection to the specified SQL Server")
      case e: Exception =>
        throw new AnalyzerRuntimeException(e.getMessage)
    }
    finally {
      if (connection.isDefined)
        connection.get.close()
    }
  }

  def withSpark(func: SparkSession => Unit): Unit = {
    val session = SparkSession.builder()
      .master("local")
      .appName("test")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    session.sparkContext.setCheckpointDir(System.getProperty("java.io.tmpdir"))

    try {
      func(session)
    } finally {
      session.stop()
      System.clearProperty("spark.driver.port")
    }
  }

}


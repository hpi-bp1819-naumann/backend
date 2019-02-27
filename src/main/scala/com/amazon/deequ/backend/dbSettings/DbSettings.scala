package com.amazon.deequ.backend.dbSettings
import java.io.FileOutputStream
import java.util.Properties

import com.amazon.deequ.backend.utils.JdbcUtils.getClass

import scala.io.Source
import java.io.FileOutputStream

object DbSettings {

  private val url = getClass.getResource("/jdbc.properties")

  if (url == null) {
    throw new IllegalStateException("Unable to find jdbc.properties in src/main/resources!")
  }

  private val properties = new Properties()
  properties.load(Source.fromURL(url).bufferedReader())

  def connectionProperties = properties

  // Getter and Setter for database uri
  def dburi = properties.getProperty("dburi")
  def dburi(value:String)= {
    properties.setProperty("dburi", value)
    properties.store(new FileOutputStream(url.getPath), null)
  }

  // Getter and Setter for database user
  def dbuser = properties.getProperty("user")
  def dbuser(value:String) = {
    properties.setProperty("user", value)
    properties.store(new FileOutputStream(url.getPath), null)
  }

  //  Setter for database password
  def dbpass(value:String)= {
    properties.setProperty("password", value)
    properties.store(new FileOutputStream(url.getPath), null)
  }
}

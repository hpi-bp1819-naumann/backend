package com.amazon.deequ.backend.utils

abstract class DBAccessPreconditionException(message: String)
  extends Exception(message)

case class NoSuchTableException(message: String)
  extends DBAccessPreconditionException(message)

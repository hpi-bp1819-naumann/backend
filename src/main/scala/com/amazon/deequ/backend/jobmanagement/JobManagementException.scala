package com.amazon.deequ.backend.jobmanagement

abstract class JobManagementException(message: String)
  extends Exception(message)

class NoSuchAnalyzerException(message: String)
  extends JobManagementException(message)

class NoSuchContextException(message: String)
  extends JobManagementException(message)

class RequestParamsException(message: String)
  extends JobManagementException(message)


abstract class JobManagementRuntimeException(message: String)
  extends Exception(message)

class AnalyzerRuntimeException(message: String)
  extends JobManagementRuntimeException(message)

class SQLConnectionException(message: String)
  extends JobManagementRuntimeException(message)
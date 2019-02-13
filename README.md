# Backend

> Backend for [deequ](https://github.com/hpi-bp1819-naumann/deequ)

## Build Setup

1. Install `sbt`
2. Clone project in the same directory as [deequ](https://github.com/hpi-bp1819-naumann/deequ)
    * Install [deequ](https://github.com/hpi-bp1819-naumann/deequ) if you haven't done so yet
3. Change to `backend` directory
4. Run `importDeequFatJar.sh`

## Start the server

1. Via IntelliJ
* Run `src/main/scala/com/amazon/deequ/backend/JettyLauncher.scala`

2. Via command line
``` bash
sbt
# in the sbt shell
jetty; start
```
The server will then run on `localhost:8080`.

## Frontend Tool

Use the [frontend](https://github.com/hpi-bp1819-naumann/frontend) for easy use.

 # API

 ## Job Management
* [List Analyzers:](./doc/analyzers.md) `GET /api/jobs/analyzers`
* [Start Analyzer:](./doc/analyzer_start.md) `POST /api/jobs/:analyzer/start`
* [List Jobs:](./doc/jobs.md) `GET /api/jobs/`
* [Job information:](./doc/job_information.md) `GET /api/jobs/:jobId`
* [Delete Job:](./doc/job_delete.md) `DELETE /api/jobs/:jobId`
* [Job status:](./doc/job_status.md) `GET /api/jobs/:jobID/status`
* [Job result:](./doc/job_result.md) `GET /api/jobs/:jobID/result`
* [Job runtime:](./doc/job_runtime.md) `GET /api/jobs/:jobID/runtime`

## Database Settings
* [Get URI:](./doc/settings_uri_get.md) `GET /api/settings/uri`
* [Set URI:](./doc/settings_uri_set.md) `POST /api/settings/uri`
* [Get User:](./doc/settings_user_get.md) `GET /api/settings/user`
* [Set User:](./doc/settings_user_set.md) `POST /api/settings/user`
* [Set Password:](./doc/settings_password_set.md) `POST /api/settings/password`

## Database Access
* [List tables:](./doc/db_tables.md) `GET /api/db/tables`
* [List columns:](./doc/db_columns.md) `GET /api/db/columns`
* [List schemas:](./doc/db_schemas.md) `GET /api/db/schemas`
* [Metadata:](./doc/db_data.md) `GET /api/db/data`
* [Table Metadata + 10 rows:](./doc/db_data_table.md) `GET /api/db/data/:table`
* [List 10 rows:](./doc/db_rows_table.md) `GET /api/db/rows/:table`
* [List n rows:](./doc/db_rows_table_n.md) `GET /api/db/rows/:table/:n`
* [Get db version:](./doc/db_version_product.md) `GET /api/db/version/:product` 
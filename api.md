 # Deequ Backend

 ## Setup
 * Clone project
 * install sbt
 * move compiled deequ .jar to /lib folder (`importDeequFatJar.sh` may help you)
 * run `sbt` in IntelliJ Terminal
 * run JettyLauncher.scala in IntelliJ

 ## API
 After starting the Frontend you can interact with it using the following API calls:

 ### Job Management
* [List Analyzers:](./doc/analyzers.md) `GET /api/jobs/analyzers`
* [Start Analyzer:](./doc/analyzer_start.md) `POST /api/jobs/:analyzer/start`
* [List Jobs:](./doc/jobs.md) `GET /api/jobs/`
* [Job information:](./doc/job_information.md) `GET /api/jobs/:jobId`
* [Delete Job:](./doc/job_delete.md) `DELETE /api/jobs/:jobId`
* GET `/api/jobs/:jobID/status`: get status of a specific job 
* GET `/api/jobs/:jobID/result`: get result of a specific job
* GET `/api/jobs/:jobID/runtime`: get the difference between finishing and start time of a specific job

### Database Settings
* GET `/api/settings/uri`: get database uri
* POST `/api/settings/uri`: set database uri, the body has to look like this:
```
{"uri": "<uri>"}
```
* GET `/api/settings/user`: get database user
* POST `/api/settings/user`: set database user, the body has to look like this:
```
{"user": "<USERNAME>"}
```
* POST `/api/settings/password`: set database password, the body has to look like this:
```
{"password": "<PASSWORD>"}
```

### Database Access
* [List tables](./doc/db_tables.md) `GET /api/db/tables`
* [List columns](./doc/db_columns.md) `GET /api/db/columns`
* [List schemas](./doc/db_schemas.md) `GET /api/db/schemas`
* [Metadata](./doc/db_data.md) `GET /api/db/data`
* [Table Metadata + 10 rows](./doc/db_data_table.md) `GET /api/db/data/:table`
* [List 10 rows](./doc/db_rows_table.md) `GET /api/db/rows/:table`
* [List n rows](./doc/db_rows_table_n.md) `GET /api/db/rows/:table/:n`
* [Get db version:](./doc/db_version_product.md) `GET /api/db/version/:product` 

               
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
* [Job status:](./doc/job_status.md) `GET /api/jobs/:jobID/status`
* [Job result:](./doc/job_result.md) `GET /api/jobs/:jobID/result`
* [Job runtime:](./doc/job_runtime.md) `GET /api/jobs/:jobID/runtime`

### Database Settings
* [Get Database URI:](./doc/dburi_get.md) `GET /api/settings/uri`
* [Set Database URI:](./doc/dburi_set.md) `POST /api/settings/uri`: set database uri, the body has to look like this:
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
* GET `/api/db/tables`: get the names of all tables 
* GET `/api/db/columns`: get the names of all columns of all tables
* GET `/api/db/schemas`: get the names of all schemas
* GET `/api/db/data`: get each table name with all column names with their datatypes
* GET `/api/db/data/:table`: get for a specific table name all column names with their datatypes and exemplary the first 10 rows of the table
* GET `/api/db/rows/:table`: get exemplary the first 10 rows of the table
* GET `/api/db/rows/:table/:n`: get exemplary the first n rows of the table
* GET `/api/db/version/:product`: get the version number of the used product, accepted product names are "jdbc" for the jdbc driver version and "db" for the database name and version number
               

               
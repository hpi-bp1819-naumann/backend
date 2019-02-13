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
* GET `/api/jobs/analyzers`: get a list of all available analyzers
* POST `/api/jobs/:analyzer/start`: start a job for a specific analyzer <br> the body has to look like this:
```
{"context": "<jdbc or spark>", "table": "<tablename>", "column": "<columnname>"}
```
* GET `/api/jobs/`: get all existing jobs
* GET `/api/jobs/:jobId`: get status, startingTime, finishingTime and result of a specific job
* DELETE `/api/jobs/:jobId`: delete a specific job
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
* GET `/api/db/tables`: get the names of all tables 
* GET `/api/db/columns`: get the names of all columns of all tables
* GET `/api/db/schemas`: get the names of all schemas
* GET `/api/db/data`: get each table name with all column names with their datatypes
* GET `/api/db/data/:table`: get for a specific table name all column names with their datatypes and exemplary the first 10 rows of the table
* GET `/api/db/rows/:table`: get exemplary the first 10 rows of the table
* GET `/api/db/rows/:table/:n`: get exemplary the first n rows of the table
* GET `/api/db/version/:product`: get the version number of the used product, accepted product names are "jdbc" for the jdbc driver version and "db" for the database name and version number
               

               
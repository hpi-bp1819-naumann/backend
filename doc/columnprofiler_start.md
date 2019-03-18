# Start Column Profiler
Starts the Column Profiler which analyzes all columns of the
given table. Is handled like an analyzer.

__URL:__ `/api/jobs/columnProfiler/start`  
__Method:__ `POST`  

__Body constraints__
```
{"context": "spark", "table": <table_name>}
```

## Response

__Content__
```
{
    "message": "Successfully started job",
    "jobId": "a2fbd8013d82445db56c60ed9a305aed"
}
```
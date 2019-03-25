# Start Column Profiler
Run the columnProfiler for a given table

__URL:__ `/api/jobs/:table/startColumnProfiler`  
__Method:__ `POST`  

__Data constraints__
```
:table - table, the columnProfiler will be run on
```

## Response

__Content__
```
{
    "message": "Successfully started job",
    "jobId": "0229d1f416ce447b963d95ccaf17f59e"
}
```
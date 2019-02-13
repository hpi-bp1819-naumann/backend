# Start Analyzer
Start a job for a specific analyzer

__URL:__ `/api/jobs/:analyzer/start`  
__Method:__ `POST`  

__Data constraints__
```
:analyzer - valid analyzer key
```

__Body constraints__
```
{"context": <"jdbc" or "spark">, "table": <"tablename">, "column": <"columnname">}
```

## Response

__Content__
```
{
    "message": "Successfully started job",
    "analyzer": "minimum",
    "jobId": "dc94aec7d57e409992c2c0a1ed5108f7"
}
```
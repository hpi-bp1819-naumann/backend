# Column Profile Information
Get a specific columnProfile

__URL:__ `/api/jobs/columnProfilerJobs/:jobId`  
__Method:__ `GET`  

__Data constraints__
```
:jobId - valid jobId of a columnProfilerJob
```

## Response

__Content__
```
{
    "result": {
        "result": {
            "columns": [
                ...
            ]
        },
        "id": "802efc2ec21c438280dda2bd0193716d",
        "finishingTime": 1553529712047,
        "status": "completed",
        "startingTime": 1553529704701
    }
}
```
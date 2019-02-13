# Job information
Get status, startingTime, finishingTime and result of a specific job

__URL:__ `/api/jobs/:jobId`  
__Method:__ `GET`  

__Data constraints__
```
:jobID - valid jobId
```

## Response

__Content__
```
{
    "job": {
        "name": "Maximum",
        "result": {
            "value": 9.04
        },
        "params": {
            "context": "jdbc",
            "table": "food_des",
            "column": "fat_factor"
        },
        "id": "80a62f199b2844fabcf63817eb30f25b",
        "finishingTime": 1550049800514,
        "status": "completed",
        "startingTime": 1550049800229
    }
}
```
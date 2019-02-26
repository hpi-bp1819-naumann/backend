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
        "name": "Minimum",
        "result": {
            "value": 0.84
        },
        "params": {
            "context": "jdbc",
            "table": "food_des",
            "column": "fat_factor",
            "where": "fat_factor > 0"
        },
        "id": "5d8bf7f7c4fd42e5a2650c37e7db2853",
        "finishingTime": 1551186364199,
        "status": "completed",
        "startingTime": 1551186364164
    }
}
```
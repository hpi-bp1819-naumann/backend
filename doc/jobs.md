# List Jobs
Get all existing jobs

__URL:__ `/api/jobs/`  
__Method:__ `GET`  


## Response

__Content__
```
{
    "jobs": [
        {
            "name": "Maximum",
            "result": {
                "value": 9.04
            },
            "id": "80a62f199b2844fabcf63817eb30f25b",
            "finishingTime": 1550049800514,
            "status": "completed",
            "startingTime": 1550049800229
        },
        ...
    ]
}
```
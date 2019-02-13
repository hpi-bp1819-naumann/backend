# Job status
Get status of a specific job 

__URL:__ `/api/jobs/:jobID/status`  
__Method:__ `GET`  

__Data constraints__
```
:jobID - valid jobID
```

## Response

__Content__
```
{
    "jobId": "dc94aec7d57e409992c2c0a1ed5108f7",
    "status": "completed"
}
```
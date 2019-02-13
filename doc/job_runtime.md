# Job runtime
Get the difference between finishing and start time of a specific job

__URL:__ `/api/jobs/:jobID/runtime`  
__Method:__ `GET`  

__Data constraints__
```
jobID - valid jobID
```


## Response

__Content__
```
{
    "jobId": "d20915c69a0040838b9da729a55815de",
    "runtime": "33"
}
```
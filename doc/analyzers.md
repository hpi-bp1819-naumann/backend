# List Analyzers
Get a list of all available analyzers

__URL:__ `/api/jobs/analyzers`  
__Method:__ `GET`  

## Response

__Content example__
```
{
    "analyzers": [
        {
            "name": "Completeness",
            "key": "completeness",
            "description": "description for completeness analyzer",
            "parameters": [
                {
                    "name": "context",
                    "type": "String"
                },
                {
                    "name": "table",
                    "type": "String"
                },
                {
                    "name": "column",
                    "type": "String"
                },
                {
                    "name": "where",
                    "type": "Option"
                }
            ]
        },
        ...
    ]
}

```
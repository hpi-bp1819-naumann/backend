# Get db version
Get the version number of the used product

__URL:__ `/api/db/version/:product`
__Method:__ `GET`  

__Data constraints__
```
:product - "jdbc" or "db"
```

## Response

__Content__
```
{"db":"<db version>"}
or
{"jdbc":"<jdbc version>"}
```
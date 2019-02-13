# Headline
Get the version number of the used product, accepted product names are "jdbc" for the jdbc driver version and "db" for the database name and version number

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
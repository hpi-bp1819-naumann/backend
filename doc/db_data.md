# Metadata
Get each table name with all column names with their datatypes
 
__URL:__ `/api/db/data/`
__Method:__ `GET`  

## Response

__Content__
```
[{
  "table":"<tablename1>",
  "columns":[{
    "name":"<columnname1>",
    "dataType":"<datatype>"
  }, {
    "name":"<columnname2>",
    "dataType":"<datatype>"
  },
  ...]
}, {
  "table":"<tablename2>",
  "columns":[...]
},
...]
```
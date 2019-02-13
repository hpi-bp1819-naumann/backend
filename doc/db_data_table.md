# Table Metadata + 10 rows
Get for a specific table name all column names with their datatypes and exemplary the first 10 rows of the table

__URL:__ `/api/db/data/:table`
__Method:__ `GET`  

__Data constraints__
```
:table - the name of the table
```

## Response

__Content__
```
{
  "table":"<tablename>",
  "columns":[{
    "name":"<columnname1>",
    "dataType":"<Datatype>"
  }, {
    "name":"<columnname2>",
    "dataType":"<datatype>"
  },
  ...],
  "rows":[
    ["<value1 of column1>","<value1 of column2>",...],
    ["<value2 of column1>","<value2 of column2>",...],
    ...
  ]
}
```
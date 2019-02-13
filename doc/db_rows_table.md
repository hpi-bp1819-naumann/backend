# List 10 rows
Get exemplary the first 10 rows of the table

__URL:__ `/api/db/rows/:table`
__Method:__ `GET`  

__Data constraints__
```
:table - the name of the table
```

## Response

__Content__
```
[
  ["<value1 of column1>","<value1 of column2>",...],
  ["<value2 of column1>","<value2 of column2>",...],
  ...
]
```
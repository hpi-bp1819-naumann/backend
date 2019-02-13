# List n rows
Get exemplary the first n rows of the table

__URL:__ `/api/db/rows/:table/:n`
__Method:__ `GET`  

__Data constraints__
```
:table - the name of the table
:n - the number of rows
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
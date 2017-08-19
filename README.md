# AsyncMysql [![Build Status](https://travis-ci.org/tulayang/asyncmysql.svg?branch=master)](https://travis-ci.org/tulayang/asyncmysql)

## Features

* single SQL statement in a query
* multiple SQL statements in a query
* streaming query for big result set
* connection pool (TODO)
* an incremental (MySQL protocol) packet parser which is independent from IO tools

### Query with a single statement:

```nim
import asyncmysql, asyncdispatch, net

proc main() {.async.} =
  var conn = await open(domain = AF_INET, 
                        port = Port(3306), 
                        host = "127.0.0.1", 
                        user = "mysql", 
                        password = "123456", 
                        database = "mysql")
                        
  var (packet, rows) = await conn.execQueryOne(
      sql("select host, user from user where user = ?", "root")) 
  echo ">>> select host, user from user where user = root"
  echo packet
  echo rows

waitFor main()
```

### Query with multiple statements:

```nim
import asyncmysql, asyncdispatch, net

proc main() {.async.} =
  var conn = await open(domain = AF_INET, 
                        port = Port(3306), 
                        host = "127.0.0.1", 
                        user = "mysql", 
                        password = "123456", 
                        database = "mysql")

  var stream = await conn.execQuery(sql("""
start transaction;
select host, user from user where user = ?;
select user from user;
commit;
""", "root"))

  let (packet0, _) = await stream.read()
  echo ">>> strart transaction;"
  echo packet0
  assert stream.finished == false
  assert packet0.kind == rpkOk
  assert packet0.hasMoreResults == true

  let (packet1, rows1) = await stream.read()
  echo ">>> select host, user from user where user = ?;"
  echo packet1
  echo rows
  assert stream.finished == false
  assert packet1.kind == rpkResultSet
  assert packet1.hasMoreResults == true

  let (packet2, rows2) = await stream.read()
  echo ">>> select user from user;"
  echo packet2
  echo rows2
  assert stream.finished == false
  assert packet2.kind == rpkResultSet
  assert packet2.hasMoreResults == true

  let (packet3, _) = await stream.read()
  echo ">>> commit;"
  echo packet3
  assert stream.finished == true
  assert packet3.kind == rpkOk
  assert packet3.hasMoreResults == false

waitFor main()
```

## TODO 

* connection pool
* API Documentation







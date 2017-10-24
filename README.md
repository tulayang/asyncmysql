# AsyncMysql [![Build Status](https://travis-ci.org/tulayang/asyncmysql.svg?branch=master)](https://travis-ci.org/tulayang/asyncmysql)

AsyncMysql is an asynchronous (None-Blocking) MySQL connector written in pure Nim.

## Features

* multiple SQL statements in a query
* streaming big result query
* transaction commit and rollback supported
* connection pool 

## Examples

```nim
proc execMyQuery(pool: AsyncMysqlPool, q: SqlQuery): Future[void] =
  var retFuture = newFuture[void]("execMyQuery")
  result = retFuture

  proc finishCb(
    err: ref Exception, 
    replies: seq[tuple[packet: ResultPacket, rows: seq[string]]]
  ) {.async.} =
    echo "Got result set"
    complete(retFuture)

  execQuery(pool, q, finishCb)

proc main() {.async.} =
  let pool = await openMysqlPool(
    domain=AF_INET, 
    port=Port(3306), 
    host="127.0.0.1", 
    user="mysql", 
    password="123456", 
    database="mysql", 
    capacity=10)
  await pool.execMyQuery(sql("""
start transaction;
select host, user from user where user = ?;
select user from user;
commit;
""", "root"))
  pool.close()
```

* API Documentation (TODO)
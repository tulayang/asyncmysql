# AsyncMysql [![Build Status](https://travis-ci.org/tulayang/asyncmysql.svg?branch=master)](https://travis-ci.org/tulayang/asyncmysql)

AsyncMysql is an asynchronous (None-Blocking) MySQL connector written in pure Nim.

## Features

* execute multiple SQL statements in a single query
* streaming large result sets
* commit and rollback transaction
* connection pool 

## Examples

```nim
proc execMyQuery(pool: AsyncMysqlPool, q: SqlQuery): Future[void] =
  var retFuture = newFuture[void]("execMyQuery")
  result = retFuture

  proc finishCb(err: ref Exception, 
                replies: seq[tuple[packet: ResultPacket, rows: seq[string]]]) {.async.} =
    if err == nil:
      echo ">> start transaction;"
      assert replies[0].packet.kind = rpkOk 

      echo ">> select host, user from user where user = ?;"
      assert replies[1].packet.kind = rpkResultSet
      echo replies[1].rows 

      echo ">> select user from user;"
      assert replies[2].packet.kind = rpkResultSet
      echo replies[2].rows 

      echo ">> commit;"
      assert replies[3].packet.kind = rpkOk

      complete(retFuture)
    else:
      fail(retFuture, err)

  execQuery(pool, q, finishCb)

proc main() {.async.} =
  let pool = await openMysqlPool(domain=AF_INET, 
                                 port=Port(3306), 
                                 host="127.0.0.1", 
                                 user="mysql", 
                                 password="123456", 
                                 database="mysql", 
                                 capacity=10)
  let query = sql("""
start transaction;
select host, user from user where user = ?;
select user from user;
commit;
""", "root")
  await pool.execMyQuery(query)
  pool.close()
```

* API Documentation (TODO)
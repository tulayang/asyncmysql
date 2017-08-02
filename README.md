# AsyncMysql [![Build Status](https://travis-ci.org/tulayang/asyncmysql.svg?branch=master)](https://travis-ci.org/tulayang/asyncmysql)

## Features

* an incremental (MySQL protocol) packet parser which is independent from IO tools 
* single SQL statement in one query
* multiple SQL statements in one query
* big data query via streaming oprations (TODO)
* connection pool (TODO)

### Query with a single statement:

```nim
proc main() {.async.} =
  var conn = await open(AF_INET, Port(3306), "127.0.0.1", 
      "root", "password123456", "mysql")
  var packet = await queryOne(conn, 
      sql("select host, user from mysql where user = ?", "root")) 

waitFor main()
```

### Query with multiple statements:

```nim
proc main() {.async.} =
  var conn = await open(AF_INET, Port(3306), "127.0.0.1", 
                        "root", "password123456", "mysql")
  var stream = await conn.query(sql("""
start transaction;
select host, user from user where user = ?;
insert into test (name) values (?);
commit;
""", "root", "demo"))

  let packet0 = await read(stream, conn)
  assert stream.finished = false
  assert packet0.kind == rpkOk
  assert packet0.hasMoreResults == true

  let packet0 = await read(stream, conn)
  assert stream.finished = false
  assert packet1.kind == rpkResultSet
  assert packet1.hasMoreResults == true

  let packet0 = await read(stream, conn)
  assert stream.finished = false
  assert packet2.kind == rpkOk
  assert packet2.hasMoreResults == true

  let packet0 = await read(stream, conn)
  assert stream.finished = true
  assert packet3.kind == rpkOk
  assert packet3.hasMoreResults == false

waitFor main()
```

## TODO 

Currently, the packet parser has been completed. The ``PacketParser`` is an excellent incremental parser which is not limited by the size of the buffer. For example, using a buffer of 16 bytes buffer to request the mysql server.

TODO: building connection pools and big-data (query) streaming tools.






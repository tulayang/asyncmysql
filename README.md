# AsyncMysql [![Build Status](https://travis-ci.org/tulayang/asyncmysql.svg?branch=master)](https://travis-ci.org/tulayang/asyncmysql)

Currently, the packet parser has been completed. The ``PacketParser`` is an excellent incremental parser which is not limited by the size of the buffer. For example, using a buffer of 16 bytes buffer to request the mysql server.

Query with single statement:

```nim
proc main() {.async.} =
  var conn = await open(AF_INET, Port(3306), "127.0.0.1", 
      "root", "password123456", "mysql")
  var packet = await queryOne(conn, 
      sql("select host, user from mysql where user = ?", "root")) 

waitFor main()
```

Query with multiple statements:

```nim
proc main() {.async.} =
  var conn = await open(AF_INET, Port(3306), "127.0.0.1", 
      "root", "password123456", "mysql")
  var stream = await query(conn, sql("""
start transaction;
select host, user from user where user = ?;
insert into test (name) values (?);
commit;
""", "root", "demo"))

  await write(stream, conn)
  let (streaming0, packet0) = await read(stream)
  assert packet0.kind == rpkOk
  assert packet0.hasMoreResults == true

  await write(stream, conn)
  let (streaming1, packet1) = await read(stream)
  assert packet1.kind == rpkResultSet
  assert packet1.hasMoreResults == true

  await write(stream, conn)
  let (streaming2, packet2) = await read(stream)
  assert packet2.kind == rpkOk
  assert packet2.hasMoreResults == true

  await write(stream, conn)
  let (streaming3, packet1) = await read(stream)
  assert packet3.kind == rpkOk
  assert packet3.hasMoreResults == false

waitFor main()
```



TODO: building connection and query tools and connection pools.


```nim
var conn = newAsyncMysqlConnection("127.0.0.1", Port(3306))
let ret = await conn.query(sql"""
  start transaction;
  select * from table1 where id = ?;
  insert into table2 (name) values (?);
  commit;
""", 100, "Twitter")
assert ret[0].kind == rpkOk
assert ret[1].kind == rpkResultSet
assert ret[2].kind == rpkOk
assert ret[3].kind == rpkOk
```

# AsyncMysql [![Build Status](https://travis-ci.org/tulayang/asyncmysql.svg?branch=master)](https://travis-ci.org/tulayang/asyncmysql)

## Features

* multiple SQL statements in a query
* big result query by streaming interface
* transaction commit and rollback supported
* connection pool (TODO)

### Simple query:

```nim
import asyncmysql, asyncdispatch, net

proc main() {.async.} =
  var conn = await open(domain = AF_INET, 
                        port = Port(3306), 
                        host = "127.0.0.1", 
                        user = "mysql", 
                        password = "123456", 
                        database = "mysql")
                        
  var resultLoad = await conn.execQuery(
      sql("select host, user from user where user = ?", "root")) 

  echo ">>> select host, user from user where user = root"
  echo resultLoad[0].packet
  echo resultLoad[0].rows

  assert resultLoad.len == 1

waitFor main()
```

### Transaction and rollback:

```nim
import asyncmysql, asyncdispatch, net

proc main() {.async.} =
  var conn = await open(domain = AF_INET, 
                        port = Port(3306), 
                        host = "127.0.0.1", 
                        user = "mysql", 
                        password = "123456", 
                        database = "mysql")

  var resultLoad = await conn.execQuery(sql("""
start transaction;                                # ok
select host, user from user-user where user = ?;  # error
select user from user;                            # not exec
commit;                                           # not exec
""", "root"))

  let resultLoad = await stream.read()
  assert resultLoad.len == 2

  if resultLoad.len < 4 or 
     (resultLoad[0].packet.kind == rpkError or 
      resultLoad[1].packet.kind == rpkError or
      resultLoad[2].packet.kind == rpkError or
      resultLoad[3].packet.kind == rpkError or):
    var resultLoad2 = await conn.execQuery(sql("""
rollback;                                         # ok     
"""))  
    assert resultLoad2.len == 1  
   
  close(conn)

waitFor main()
```

* API Documentation (TODO)
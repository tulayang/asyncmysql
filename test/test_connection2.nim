#    AsyncMysql - Asynchronous MySQL connector written in pure Nim
#        (c) Copyright 2017 Wang Tong
#
#    See the file "LICENSE", included in this distribution, for
#    details about the copyright.

import unittest, asyncmysql, util, mysqlparser, asyncdispatch, asyncnet, net

const 
  MysqlHost = "127.0.0.1"
  MysqlPort = Port(3306)
  MysqlUser = "mysql"
  MysqlPassword = "123456"

suite "AsyncMysqlConnection":
  test "query multiple":
    proc sendComQuery() {.async.} =
      var conn = await open(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      var stream = await execQuery(conn, sql("""
start transaction;
select host, user from user where user = ?;
select user from user;
commit;
""", "root"))

      let (packet0, _) = await read(stream)
      echo "  >>> strart transaction;"
      echo "  ", packet0
      echo stream.finished, " ", packet0.kind, " ", packet0.hasMoreResults
      check stream.finished == false
      check packet0.kind == rpkOk
      check packet0.hasMoreResults == true

      let (packet1, rows1) = await read(stream)
      echo "  >>> select host, user from user where user = ?;"
      echo "  ", packet1
      echo "  ", rows1
      check stream.finished == false
      check packet1.kind == rpkResultSet
      check packet1.hasMoreResults == true

      let (packet2, rows2) = await read(stream)
      echo "  >>> select user from user;"
      echo "  ", packet2
      echo "  ", rows2
      check stream.finished == false
      check packet2.kind == rpkResultSet
      check packet2.hasMoreResults == true

      let (packet3, _) = await read(stream)
      echo "  >>> commit;"
      echo "  ", packet3
      check stream.finished == true
      check packet3.kind == rpkOk
      check packet3.hasMoreResults == false

      close(conn)
    waitFor1 sendComQuery() 

  test "query multiple with bad results":
    proc sendComQuery() {.async.} =
      var conn = await open(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      var stream = await execQuery(conn, sql("""
select 100;
select var;
select 10;
""", "root"))
      
      let (packet0, _) = await read(stream)
      echo "  >>> select 100;"
      echo "  ", packet0
      check stream.finished == false
      check packet0.kind == rpkResultSet
      check packet0.hasMoreResults == true

      let (packet1, _) = await read(stream)
      echo "  >>> select var;"
      echo "  ", packet1
      check stream.finished == true
      check packet1.kind == rpkError
      check packet1.hasMoreResults == false

      close(conn)
    waitFor1 sendComQuery() 

  test "query one":
    proc sendComQuery() {.async.} =
      var conn = await open(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      let (packet, rows) = await execQueryOne(conn, sql("select 100"))
      echo "  >>> select 100;"
      echo "  ", packet
      echo "  ", rows
      check packet.kind == rpkResultSet
      check packet.hasMoreResults == false
      close(conn)
    waitFor1 sendComQuery() 

  test "query big-one":
    proc sendComQuery() {.async.} =
      var conn = await open(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      let (packet, stream) = await execQueryBigOne(conn, sql("select 100"))
      var rows: seq[string] = @[]
      var buf = newString(16)
      while true:
        let (offset, state) = await read(stream, buf.cstring, buf.len)
        case state
        of bigFieldBegin:
          setLen(buf, 16)
        of bigFieldFull:
          check false
        of bigFieldEnd:
          add(rows, buf[0..offset-1])
        of bigFinished:
          break
      echo "  >>> select 100;"
      echo "  ", packet
      echo "  ", rows
      check packet.kind == rpkResultSet
      check packet.hasMoreResults == false
      close(conn)
    waitFor1 sendComQuery() 

  test "when there are multiple requests are at the same time, the requests are queued":
    var conn: AsyncMysqlConnection

    proc sendComQuery1() {.async.} =  
      let (packet, rows) = await execQueryOne(conn, sql("select 100"))
      echo "  >>> select 100;"
      echo "  ", packet
      echo "  ", rows
      check packet.kind == rpkResultSet
      check packet.hasMoreResults == false

    proc sendComQuery2() {.async.} =
      let (packet, rows) = await execQueryOne(conn, sql("select 200"))
      echo "  >>> select 200;"
      echo "  ", packet
      echo "  ", rows
      check packet.kind == rpkResultSet
      check packet.hasMoreResults == false

    proc doQueries(): Future[void] =
      var retFuture = newFuture[void]("")
      result = retFuture
      var n = 2
      var fut1 = sendComQuery1()
      fut1.callback = proc () = 
        dec(n)
        if fut1.failed:
          raise fut1.readError()
        if n == 0:
          complete(retFuture)
      var fut2 = sendComQuery2()
      fut2.callback = proc () = 
        dec(n)
        if fut2.failed:
          raise fut2.readError()
        if n == 0:
          complete(retFuture)

    proc sendComQuery() {.async.} =
      conn = await open(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      await doQueries()

    waitFor1 sendComQuery() 

  test "ping":
    proc sendComPing() {.async.} =
      var conn = await open(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      let packet = await execPing(conn)
      echo "  >>> ping;"
      echo "  ", packet
      check packet.kind == rpkOk
      check packet.hasMoreResults == false
      close(conn)
    waitFor1 sendComPing()  

  test "use <database>":
    proc sendComQuery() {.async.} =
      var conn = await open(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")

      let (packet0, _) = await execQueryOne(conn, sql"use test")
      echo "  >>> use test;"
      echo "  ", packet0
      check packet0.kind == rpkOk
      check packet0.hasMoreResults == false

      let (packet1, _) = await execQueryOne(conn, sql("select * from user;"))
      echo "  >>> select * from user;"
      echo "  ", packet1
      check packet1.kind == rpkError
      check packet0.hasMoreResults == false

      close(conn)
    waitFor1 sendComQuery()  

  test "show full fields from <table>":
    proc sendComQuery() {.async.} =
      var conn = await open(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      let (packet0, rows) = await execQueryOne(conn, sql"show full fields from user;")
      echo "  >>> show full fields from user;"
      echo "  ... ", packet0.fields[0], " ..."
      echo "  ... ", rows[0], " ..."
      check packet0.kind == rpkResultSet
      check packet0.hasMoreResults == false
      close(conn)
    waitFor1 sendComQuery()   

  test "change user":
    proc sendComChangeUser() {.async.} =
      var conn = await open(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      let packet0 = await execChangeUser(conn, "mysql2", MysqlPassword, "mysql", DefaultClientCharset)
      echo "  >>> change user;"
      echo "  ", packet0
      check packet0.kind == rpkOk
      check packet0.hasMoreResults == false
      close(conn)
    waitFor1 sendComChangeUser()   

  test "quit":
    proc sendComQuit() {.async.} =
      var conn = await open(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      await execQuit(conn)
      close(conn)
    waitFor1 sendComQuit()   


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
  test "query by streaming fields":
    proc sendComQuery() {.async.} =
      let conn = await open(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      var qIx= 0

      proc packetCb(packet: ResultPacket): Future[void] {.async.} =
        inc(qIx)
        case qIx
        of 1: 
          echo "  >>> strart transaction;"
          echo "  ", packet
          check packet.kind == rpkOk
        of 2: 
          echo "  >>> select host, user from user where user = ?;"
          echo "  ", packet
          write(stdout, "  ")
          check packet.kind == rpkResultSet
        of 3: 
          echo "  >>> select user from user;"
          echo "  ", packet
          write(stdout, "  ")
          check packet.kind == rpkResultSet
        of 4: 
          echo "  >>> commit;"
          echo "  ", packet
          check packet.kind == rpkOk
        else:
          discard

      proc packetEndCb(): Future[void] {.async.} =
        case qIx
        of 1: 
          discard
        of 2: 
          write(stdout, "\n")
        of 3: 
          write(stdout, "\n")
        of 4: 
          discard
        else:
          discard

      proc fieldCb(field: string): Future[void] {.async.} =
        case qIx
        of 1: 
          discard
        of 2: 
          write(stdout, field, " ")
        of 3: 
          write(stdout, field, " ")
        of 4: 
          discard
        else:
          discard

      await execBigQuery(conn, sql("""
start transaction;
select host, user from user where user = ?;
select user from user;
commit;
""", "root"), packetCb, packetEndCb, fieldCb)

      close(conn)

    waitFor1 sendComQuery() 

  test "query by streaming 3 bytes of field buffer":
    proc sendComQuery() {.async.} =
      let conn = await open(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      var qIx= 0

      proc packetCb(packet: ResultPacket): Future[void] {.async.} =
        inc(qIx)
        case qIx
        of 1: 
          echo "  >>> strart transaction;"
          echo "  ", packet
          check packet.kind == rpkOk
        of 2: 
          echo "  >>> select host, user from user where user = ?;"
          echo "  ", packet
          write(stdout, "  ")
          check packet.kind == rpkResultSet
        of 3: 
          echo "  >>> select user from user;"
          echo "  ", packet
          write(stdout, "  ")
          check packet.kind == rpkResultSet
        of 4: 
          echo "  >>> commit;"
          echo "  ", packet
          check packet.kind == rpkOk
        else:
          discard

      proc packetEndCb(): Future[void] {.async.} =
        case qIx
        of 1: 
          discard
        of 2: 
          write(stdout, "\n")
        of 3: 
          write(stdout, "\n")
        of 4: 
          discard
        else:
          discard

      proc fieldCb(field: string): Future[void] {.async.} =
        case qIx
        of 1: 
          discard
        of 2: 
          write(stdout, field)
        of 3: 
          write(stdout, field)
        of 4: 
          discard
        else:
          discard

      proc fieldEndCb(): Future[void] {.async.} =
        case qIx
        of 1: 
          discard
        of 2: 
          write(stdout, " ")
        of 3: 
          write(stdout, " ")
        of 4: 
          discard
        else:
          discard

      await execBigQuery(conn, sql("""
start transaction;
select host, user from user where user = ?;
select user from user;
commit;
""", "root"), 3, packetCb, packetEndCb, fieldCb, fieldEndCb)

      close(conn)

    waitFor1 sendComQuery() 

  test "query by read all":
    proc sendComQuery() {.async.} =
      let conn = await open(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      let rload = await execQuery(conn, sql("""
  start transaction;
  select host, user from user where user = ?;
  select user from user;
  commit;
  """, "root"))
      check rload.len == 4

      echo "  >>> strart transaction;"
      echo "  ", rload[0].packet
      check rload[0].packet.kind == rpkOk
      check rload[0].rows == nil
     
      echo "  >>> select host, user from user where user = ?;"
      echo "  ", rload[1].packet
      echo "  ", rload[1].rows
      check rload[1].packet.kind == rpkResultSet
    
      echo "  >>> select user from user;"
      echo "  ", rload[2].packet
      echo "  ", rload[2].rows
      check rload[2].packet.kind == rpkResultSet
    
      echo "  >>> commit;"
      echo "  ", rload[3].packet
      check rload[3].packet.kind == rpkOk
      check rload[3].rows == nil

      close(conn)

    waitFor1 sendComQuery() 

  test "query and rollback":
    proc sendComQuery() {.async.} =
      let conn = await open(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      let rload = await execQuery(conn, sql("""
  start transaction;
  select host, user from useruser where user = ?;
  select user from user;
  commit;
  """, "root"))
      check rload.len == 2

      echo "  >>> strart transaction;"
      echo "  ", rload[0].packet
      check rload[0].packet.kind == rpkOk
      check rload[0].rows == nil
     
      echo "  >>> select host, user from user where user = ?;"
      echo "  ", rload[1].packet
      check rload[1].packet.kind == rpkError
      check rload[1].rows == nil

      let rloadRollback = await execQuery(conn, sql("""
  rollback;
  """, "root"))
      check rloadRollback.len == 1

      echo "  >>> rollback;"
      echo "  ", rloadRollback[0].packet
      check rloadRollback[0].packet.kind == rpkOk
      check rloadRollback[0].rows == nil

      close(conn)

    waitFor1 sendComQuery() 

  test "when there are multiple requests at the same time, the requests are queued":
    var conn: AsyncMysqlConnection

    proc sendComQuery1() {.async.} =  
      let rload = await execQuery(conn, sql("select 100"))
      echo "  >>> select 100;"
      echo "  ", rload[0].packet
      echo "  ", rload[0].rows
      check rload[0].packet.kind == rpkResultSet

    proc sendComQuery2() {.async.} =
      let rload = await execQuery(conn, sql("select 200"))
      echo "  >>> select 200;"
      echo "  ", rload[0].packet
      echo "  ", rload[0].rows
      check rload[0].packet.kind == rpkResultSet

    proc doQueries(): Future[void] =
      var retFuture = newFuture[void]("")
      result = retFuture
      var fut1 = sendComQuery1()
      var fut2 = sendComQuery2()
      fut2.callback = proc (fut2: Future[void]) = 
        check fut2.failed == false

        fut1.callback = proc (fut1: Future[void]) = 
          check fut1.failed == false
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

      let rload0 = await execQuery(conn, sql"use test")
      echo "  >>> use test;"
      echo "  ", rload0[0].packet
      check rload0[0].packet.kind == rpkOk
      check rload0[0].rows == nil

      let rload1 = await execQuery(conn, sql("select * from user;"))
      echo "  >>> select * from user;"
      echo "  ", rload1[0].packet
      check rload1[0].packet.kind == rpkError
      check rload1[0].rows == nil

      close(conn)
    waitFor1 sendComQuery()  

  test "show full fields from <table>":
    proc sendComQuery() {.async.} =
      var conn = await open(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      let rload = await execQuery(conn, sql"show full fields from user;")
      echo "  >>> show full fields from user;"
      echo "  ... ", rload[0].packet.fields[0], " ..."
      echo "  ... ", rload[0].rows[0], " ..."
      check rload[0].packet.kind == rpkResultSet
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


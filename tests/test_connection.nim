#    AsyncMysql - Asynchronous MySQL connector written in pure Nim
#        (c) Copyright 2017 Wang Tong
#
#    See the file "LICENSE", included in this distribution, for
#    details about the copyright.

import unittest, asyncmysql, util, mysqlparser, asyncdispatch, asyncnet, net, strutils

const 
  MysqlHost = "127.0.0.1"
  MysqlPort = Port(3306)
  MysqlUser = "mysql"
  MysqlPassword = "123456"

suite "AsyncMysqlConnection":
  test "streaming big query, a complete field at a time":
    proc execQuery(conn: AsyncMysqlConnection): Future[void] =
      var retFuture = newFuture[void]("test.execQuery")
      result = retFuture
      var qIx= 0

      proc recvPacketCb(packet: ResultPacket): Future[void] {.async.} =
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

      proc recvPacketEndCb(): Future[void] {.async.} =
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

      proc recvFieldCb(field: string): Future[void] {.async.} =
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

      proc finishCb(err: ref Exception): Future[void] {.async.} =
        check err == nil
        complete(retFuture)

      execQuery(conn, sql("""
start transaction;
select host, user from user where user = ?;
select user from user;
commit;
""", "root"), finishCb, recvPacketCb, recvPacketEndCb, recvFieldCb)

    proc main() {.async.} =
      let conn = await openMysqlConnection(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      await execQuery(conn)
      close(conn)

    waitFor1 main() 

  test "streaming big query, 3 bytes of field buffer":
    proc execQuery(conn: AsyncMysqlConnection): Future[void] =
      var retFuture = newFuture[void]("test.execQuery")
      result = retFuture
      var qIx= 0

      proc recvPacketCb(packet: ResultPacket): Future[void] {.async.} =
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

      proc recvPacketEndCb(): Future[void] {.async.} =
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

      proc recvFieldCb(buffer: string): Future[void] {.async.} =
        case qIx
        of 1: 
          discard
        of 2: 
          write(stdout, buffer)
        of 3: 
          write(stdout, buffer)
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

      proc finishCb(err: ref Exception): Future[void] {.async.} =
        check err == nil
        complete(retFuture)

      execQuery(conn, sql("""
start transaction;
select host, user from user where user = ?;
select user from user;
commit;
""", "root"), 3, finishCb, recvPacketCb, recvPacketEndCb, recvFieldCb, fieldEndCb)

    proc main() {.async.} =
      let conn = await openMysqlConnection(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      await execQuery(conn)
      close(conn)

    waitFor1 main() 

  test "atomic query, read all":
    proc execQuery(conn: AsyncMysqlConnection): Future[void] =
      var retFuture = newFuture[void]("test.execQuery")
      result = retFuture

      proc finishCb(
        err: ref Exception, 
        replies: seq[tuple[packet: ResultPacket, rows: seq[string]]]
      ): Future[void] {.async.} =
        check replies.len == 4

        echo "  >>> strart transaction;"
        echo "  ", replies[0].packet
        check replies[0].packet.kind == rpkOk
        check replies[0].rows == nil
       
        echo "  >>> select host, user from user where user = ?;"
        echo "  ", replies[1].packet
        echo "  ", replies[1].rows
        check replies[1].packet.kind == rpkResultSet
      
        echo "  >>> select user from user;"
        echo "  ", replies[2].packet
        echo "  ", replies[2].rows
        check replies[2].packet.kind == rpkResultSet
      
        echo "  >>> commit;"
        echo "  ", replies[3].packet
        check replies[3].packet.kind == rpkOk
        check replies[3].rows == nil

        complete(retFuture)

      execQuery(conn, sql("""
  start transaction;
  select host, user from user where user = ?;
  select user from user;
  commit;
  """, "root"), finishCb)

    proc main() {.async.} =
      let conn = await openMysqlConnection(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      await execQuery(conn)
      close(conn)

    waitFor1 main() 

  test "commit and rollback":
    proc execCreateTable(conn: AsyncMysqlConnection): Future[void] =
      var retFuture = newFuture[void]("test.execCreateTable")
      result = retFuture

      proc finishCb(
        err: ref Exception, 
        replies: seq[tuple[packet: ResultPacket, rows: seq[string]]]
      ): Future[void] {.async.} =
        check err == nil
        check replies.len == 4
        complete(retFuture)

      execQuery(conn, sql("""
  use test;
  drop table if exists sample;
  create table sample(id int unsigned not null auto_increment primary key, val int unsigned not null);
  insert into sample(val) values (100);
  """), finishCb)

    proc execRollback(conn: AsyncMysqlConnection): Future[void] = 
      var retFuture = newFuture[void]("test.execRollback")
      result = retFuture

      proc finishCb(
        err: ref Exception, 
        replies: seq[tuple[packet: ResultPacket, rows: seq[string]]]
      ): Future[void] {.async.} =
        check err == nil
        check replies.len == 1
        echo "  >>> rollback;"
        echo "  ", replies[0].packet
        check replies[0].packet.kind == rpkOk
        check replies[0].rows == nil
        complete(retFuture)

      execQuery(conn, sql("""
  rollback;
  """, "root"), finishCb)

    proc execTransaction(conn: AsyncMysqlConnection): Future[void] = 
      var retFuture = newFuture[void]("test.execTransaction")
      result = retFuture

      proc finishCb(
        err: ref Exception, 
        replies: seq[tuple[packet: ResultPacket, rows: seq[string]]]
      ): Future[void] {.async.} =
        check err == nil
        check replies.len == 4

        echo "  >>> strart transaction;"
        echo "  ", replies[0].packet
        check replies[0].packet.kind == rpkOk
        check replies[0].rows == nil
       
        echo "  >>> select val from sample where id = ?;"
        echo "  ", replies[1].packet
        check replies[1].packet.kind == rpkResultSet
        check replies[1].rows[0] == "100"
        echo "  ", replies[1].rows

        echo "  >>> update sample set val = 1 where id = ?;"
        echo "  ", replies[2].packet
        check replies[2].packet.kind == rpkOk
        check replies[2].rows == nil

        echo "  >>> insert into sample (val) values (200),,,;"
        echo "  ", replies[3].packet
        check replies[3].packet.kind == rpkError
        check replies[3].rows == nil

        await execRollback(conn)
        complete(retFuture)

      execQuery(conn, sql("""
  start transaction;
  select val from sample where id = ?;
  update sample set val = 1 where id = ?;
  insert into sample (val) values (200),,,;
  commit;
  """, "1", "1"), finishCb)

    proc execReselect(conn: AsyncMysqlConnection): Future[void] =
      var retFuture = newFuture[void]("test.execTransaction")
      result = retFuture

      proc finishCb(
        err: ref Exception, 
        replies: seq[tuple[packet: ResultPacket, rows: seq[string]]]
      ): Future[void] {.async.} =
        check err == nil
        check replies.len == 1
        echo "  ", replies[0].packet
        check replies[0].packet.kind == rpkResultSet
        check replies[0].rows[0] == "100"
        echo "  ", replies[0].rows  
        complete(retFuture)

      execQuery(conn, sql("""
  select val from sample where id = ?;
  """, "1"), finishCb)

    proc main() {.async.} =
      let conn = await openMysqlConnection(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "test")
      await execCreateTable(conn)
      await execTransaction(conn)
      await execReselect(conn)
      close(conn)

    waitFor1 main() 

  test "when there are multiple requests at the same time, the requests are queued":
    proc execQuery(conn: AsyncMysqlConnection): Future[void] =
      var retFuture = newFuture[void]("text.execQuery")
      result = retFuture
      var n = 2

      proc finish1Cb(
        err: ref Exception, 
        replies: seq[tuple[packet: ResultPacket, rows: seq[string]]]
      ): Future[void] {.async.} =
        echo "  >>> select 100;"
        echo "  ", replies[0].packet
        echo "  ", replies[0].rows
        check replies[0].packet.kind == rpkResultSet
        dec(n)
        if n == 0:
          complete(retFuture)

      proc finish2Cb(
        err: ref Exception, 
        replies: seq[tuple[packet: ResultPacket, rows: seq[string]]]
      ): Future[void] {.async.} =
        echo "  >>> select 200;"
        echo "  ", replies[0].packet
        echo "  ", replies[0].rows
        check replies[0].packet.kind == rpkResultSet
        dec(n)
        if n == 0:
          complete(retFuture)

      execQuery(conn, sql("select 100"), finish1Cb)
      execQuery(conn, sql("select 200"), finish2Cb)

    proc main() {.async.} =
      let conn = await openMysqlConnection(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      await execQuery(conn)
      close(conn)

    waitFor1 main() 

  test "ping":
    proc execPing(conn: AsyncMysqlConnection): Future[void] =
      var retFuture = newFuture[void]("text.execQuery")
      result = retFuture

      proc finishCb(err: ref Exception, reply: ResultPacket): Future[void] {.async.} = 
        echo "  >>> ping;"
        echo "  ", reply
        check err == nil
        check reply.kind == rpkOk
        check reply.hasMoreResults == false
        complete(retFuture)

      execPing(conn, finishCb)

    proc sendComPing() {.async.} =
      var conn = await openMysqlConnection(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      await execPing(conn)
      close(conn)

    waitFor1 sendComPing()  

  test "use <database>":
    proc execUse(conn: AsyncMysqlConnection): Future[void] =
      var retFuture = newFuture[void]("text.execUse")
      result = retFuture

      proc finishCb(
        err: ref Exception, 
        replies: seq[tuple[packet: ResultPacket, rows: seq[string]]]
      ): Future[void] {.async.} = 
        echo "  >>> use test;"
        echo "  ", replies[0].packet
        check err == nil
        check replies[0].packet.kind == rpkOk
        check replies[0].rows == nil
        complete(retFuture)

      execQuery(conn, sql"use test", finishCb)

    proc execSelect(conn: AsyncMysqlConnection): Future[void] =
      var retFuture = newFuture[void]("text.execSelect")
      result = retFuture

      proc finishCb(
        err: ref Exception, 
        replies: seq[tuple[packet: ResultPacket, rows: seq[string]]]
      ): Future[void] {.async.} = 
        echo "  >>> use test;"
        echo "  ", replies[0].packet
        check err == nil
        check replies[0].packet.kind == rpkError
        check replies[0].rows == nil
        complete(retFuture)

      execQuery(conn, sql"select * from user;", finishCb)

    proc sendComPing() {.async.} =
      var conn = await openMysqlConnection(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      await execUse(conn)
      await execSelect(conn)
      close(conn)

    waitFor1 sendComPing()  

    proc main() {.async.} =
      var conn = await openMysqlConnection(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      await execUse(conn)
      await execSelect(conn)
      close(conn)

    waitFor1 main()  

  test "show full fields from <table>":
    proc execQuery(conn: AsyncMysqlConnection): Future[void] =
      var retFuture = newFuture[void]("text.execQuery")
      result = retFuture

      proc finishCb(
        err: ref Exception, 
        replies: seq[tuple[packet: ResultPacket, rows: seq[string]]]
      ): Future[void] {.async.} =
        echo "  ... ", replies[0].packet.fields[0], " ..."
        echo "  ... ", replies[0].rows[0], " ..."
        check replies[0].packet.kind == rpkResultSet
        complete(retFuture)

      execQuery(conn, sql"show full fields from user;", finishCb)

    proc main() {.async.} =
      var conn = await openMysqlConnection(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      await execQuery(conn)
      close(conn)

    waitFor1 main()   

  test "change user":
    proc execChangeUser(conn: AsyncMysqlConnection): Future[void] =
      var retFuture = newFuture[void]("text.execQuery")
      result = retFuture

      proc finishCb(
        err: ref Exception, 
        reply: ResultPacket
      ): Future[void] {.async.} =
        echo "  >>> change user;"
        echo "  ", reply
        check reply.kind == rpkOk
        check reply.hasMoreResults == false
        complete(retFuture)

      execChangeUser(conn, "mysql2", MysqlPassword, "mysql", DefaultClientCharset, finishCb)

    proc main() {.async.} =
      var conn = await openMysqlConnection(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      await execChangeUser(conn)
      close(conn)

    waitFor1 main()   

  test "quit":
    proc execQuit(conn: AsyncMysqlConnection): Future[void] =
      var retFuture = newFuture[void]("text.execQuery")
      result = retFuture

      proc finishCb(
        err: ref Exception
      ): Future[void] {.async.} =
        echo "  >>> quit;"
        complete(retFuture)

      execQuit(conn, finishCb)

    proc main() {.async.} =
      var conn = await openMysqlConnection(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      await execQuit(conn)
      close(conn)

    waitFor1 main()   

  test "inserting and selecting large results":
    proc execCreateTable(conn: AsyncMysqlConnection): Future[void] =
      var retFuture = newFuture[void]("test.execCreateTable")
      result = retFuture

      proc finishCb(
        err: ref Exception,
        replies: seq[tuple[packet: ResultPacket, rows: seq[string]]]
      ): Future[void] {.async.} =
        check err == nil
        check replies.len == 4
        complete(retFuture)

      execQuery(conn, sql("""
        use test;
        drop table if exists sample;
        create table sample(id int unsigned not null auto_increment primary key, val longtext not null);
        insert into sample(val) values (?);
        """, repeatChar(1000000, 'a')), finishCb)
         
    proc execSelect(conn: AsyncMysqlConnection): Future[void] =
      var retFuture = newFuture[void]("test.execSelect")
      result = retFuture

      proc finishCb(
        err: ref Exception,
        replies: seq[tuple[packet: ResultPacket, rows: seq[string]]]
      ): Future[void] {.async.} =
        check err == nil
        check replies.len == 1
        complete(retFuture)
         
      execQuery(conn, sql("select * from sample"), finishCb)
        
    proc main() {.async.} =
      let conn = await openMysqlConnection(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "test")
      await execCreateTable(conn)
      await execSelect(conn)
      close(conn)
         
    waitFor1 main()

  test "inserting and selecting large results (streaming mode)":
    proc execCreateTable(conn: AsyncMysqlConnection): Future[void] =
      var retFuture = newFuture[void]("test.execCreateTable")
      result = retFuture

      proc finishCb(
        err: ref Exception,
        replies: seq[tuple[packet: ResultPacket, rows: seq[string]]]
      ): Future[void] {.async.} =
        check err == nil
        check replies.len == 4
        complete(retFuture)

      execQuery(conn, sql("""
        use test;
        drop table if exists sample;
        create table sample(id int unsigned not null auto_increment primary key, val longtext not null);
        insert into sample(val) values (?);
        """, repeatChar(1000000, 'a')), finishCb)
         
    proc execSelect(conn: AsyncMysqlConnection): Future[void] =
      var retFuture = newFuture[void]("test.execSelect")
      result = retFuture

      proc recvPacketCb(packet: ResultPacket): Future[void] {.async.} =
        check packet.kind == rpkResultSet

      proc recvPacketEndCb(): Future[void] {.async.} =
        discard

      proc recvFieldCb(field: string): Future[void] {.async.} =
        discard

      proc finishCb(err: ref Exception): Future[void] {.async.} =
        check err == nil
        complete(retFuture)

      execQuery(conn, sql("select * from sample"), finishCb, recvPacketCb, recvPacketEndCb, recvFieldCb)
       
    proc main() {.async.} =
      let conn = await openMysqlConnection(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "test")
      await execCreateTable(conn)
      await execSelect(conn)
      close(conn)
         
    waitFor1 main()


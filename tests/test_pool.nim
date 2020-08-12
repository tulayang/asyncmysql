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

suite "AsyncMysqlPool":
  test "open by 10 connections":
    proc main() {.async.} =
      let pool = await openMysqlPool(AF_INET, MysqlPort, MysqlHost, MysqlUser, 
                                   MysqlPassword, "mysql", connectionLimit=10)
      check pool.countAvailableConnections == 10
      close(pool)
      check pool.countAvailableConnections == 0

    waitFor1 main()

  test "streaming 2 big query":
    var coin = 1

    proc execQuery1(pool: AsyncMysqlPool): Future[void] =
      var retFuture = newFuture[void]("test.execQuery")
      result = retFuture
      var qIx= 0

      proc recvPacketCb(packet: ResultPacket) {.async.} =
        await sleepAsync(100)
        inc(qIx)
        case qIx
        of 1: 
          check packet.kind == rpkOk
        of 2: 
          check packet.kind == rpkResultSet
        of 3: 
          check packet.kind == rpkResultSet
        of 4: 
          check packet.kind == rpkOk
        else:
          discard

      proc finishCb(err: ref Exception) {.async.} =
        check err == nil
        check coin == 2
        inc(coin)
        complete(retFuture)

      execQuery(pool, sql("""
start transaction;
select host, user from user where user = ?;
select user from user;
commit;
""", "root"), finishCb, recvPacketCb, nil, nil)

    proc execQuery2(pool: AsyncMysqlPool): Future[void] =
      var retFuture = newFuture[void]("test.execQuery")
      result = retFuture
      var qIx= 0

      proc recvPacketCb(packet: ResultPacket) {.async.} =
        inc(qIx)
        case qIx
        of 1: 
          check packet.kind == rpkOk
        of 2: 
          check packet.kind == rpkResultSet
        of 3: 
          check packet.kind == rpkResultSet
        of 4: 
          check packet.kind == rpkOk
        else:
          discard

      proc finishCb(err: ref Exception) {.async.} =
        check err == nil
        check coin == 1
        inc(coin)
        complete(retFuture)

      execQuery(pool, sql("""
start transaction;
select host, user from user where user = ?;
select user from user;
commit;
""", "root"), finishCb, recvPacketCb, nil, nil)

    proc execTasks(pool: AsyncMysqlPool): Future[void] =
      var retFuture = newFuture[void]("")
      result = retFuture
      var n = 2
      proc cb() =
        dec(n)
        if n == 0:
          callSoon() do ():
            complete(retFuture)
      execQuery1(pool).callback = cb
      execQuery2(pool).callback = cb

    proc main() {.async.} =
      let pool = await openMysqlPool(AF_INET, MysqlPort, MysqlHost, MysqlUser, 
                                   MysqlPassword, "mysql", connectionLimit=10)
      await execTasks(pool)
      close(pool)

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

        echo "  >> select val from sample where id = ?;"
        echo "  ", replies[0].packet
        check replies[0].packet.kind == rpkResultSet
        check replies[0].rows[0] == "100"
        echo "  ", replies[0].rows  
        complete(retFuture)

      execQuery(conn, sql("""
  select val from sample where id = ?;
  """, "1"), finishCb)

    proc request(pool: AsyncMysqlPool): Future[void] = 
      var retFuture = newFuture[void]("test.execTransaction")
      result = retFuture

      proc RequestCb(conn: AsyncMysqlConnection, connIx: int) {.async.} =
        await execCreateTable(conn)
        await execTransaction(conn)
        await execReselect(conn)
        release(pool, connIx)
        complete(retFuture)

      request(pool, RequestCb)

    proc main() {.async.} =
      let pool = await openMysqlPool(AF_INET, MysqlPort, MysqlHost, MysqlUser, 
                                     MysqlPassword, "mysql", connectionLimit=10)
      await request(pool)
      close(pool)

    waitFor1 main() 

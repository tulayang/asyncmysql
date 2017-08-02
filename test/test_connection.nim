#    AsyncMysql - Asynchronous MySQL connector written in pure Nim
#        (c) Copyright 2017 Wang Tong
#
#    See the file "LICENSE", included in this distribution, for
#    details about the copyright.

import unittest, asyncmysql, util, asyncdispatch, asyncnet, net

const 
  MysqlHost = "127.0.0.1"
  MysqlPort = Port(3306)
  MysqlUser = "mysql"
  MysqlPassword = "123456"

suite "AsyncMysqlConnection":
  test "query multiply":
    proc sendComQuery() {.async.} =
      var conn = await open(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      var stream = await query(conn, sql("""
select host, user from user where user = ?;
select user from user;
""", "root"))

      await write(stream, conn)
      let (streaming0, packet0) = await read(stream)
      check packet0.kind == rpkResultSet
      check packet0.hasMoreResults == true
      echo "  >>> select host, user from user where user = ?;"
      echo "  ", packet0

      await write(stream, conn)
      let (streaming1, packet1) = await read(stream)
      check packet1.kind == rpkResultSet
      check packet1.hasMoreResults == false
      echo "  >>> select user from user;"
      echo "  ", packet1

    waitFor1 sendComQuery() 

  test "query multiply with bad results":
    proc sendComQuery() {.async.} =
      var conn = await open(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      var stream = await query(conn, sql("""
select 100;
select var;
select 10;
""", "root"))
      
      await write(stream, conn)
      let (streaming0, packet0) = await read(stream)
      check packet0.kind == rpkResultSet
      check packet0.hasMoreResults == true
      echo "  >>> select 100;"
      echo "  ", packet0

      await write(stream, conn)
      let (streaming1, packet1) = await read(stream)
      check packet1.kind == rpkError
      check packet1.hasMoreResults == false
      echo "  >>> select var;"
      echo "  ", packet1

      let (streaming2, packet2) = await read(stream)
      check streaming2 == false

    waitFor1 sendComQuery() 

  test "query singly":
    proc sendComQuery() {.async.} =
      var conn = await open(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      var packet = await queryOne(conn, sql("select 100"))
      check packet.kind == rpkResultSet
      check packet.hasMoreResults == false
      echo "  >>> select 100;"
      echo "  ", packet

    waitFor1 sendComQuery() 


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
select password from user;
""", "root"))

      await write(stream, conn)
      let (streaming0, packet0) = await read(stream)
      check packet0.kind == rpkResultSet
      echo "  >>> select host, user from user where user = ?;"
      echo "  ", packet0

      await write(stream, conn)
      let (streaming1, packet1) = await read(stream)
      check packet1.kind == rpkResultSet
      echo "  >>> select password from user;"
      echo "  ", packet1

      check packet1.hasMoreResults == false
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
      echo "  >>> select 100;"
      echo "  ", packet0

      await write(stream, conn)
      let (streaming1, packet1) = await read(stream)
      check packet1.kind == rpkError
      echo "  >>> select var;"
      echo "  ", packet1

      let (streaming2, packet2) = await read(stream)
      echo packet2
      # check streaming2 == false
    waitFor1 sendComQuery() 


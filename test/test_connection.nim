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
      var stream = query(conn, sql("""
select host, user from user where user = ?;
select password from user;
""", "root"))
      let (streaming0, packet0) = await await stream
      check packet0.kind == rpkResultSet
      echo "  >>> select host, user from user where user = ?;"
      echo "  ", packet0

      let (streaming1, packet1) = await await stream
      check packet1.kind == rpkResultSet
      echo "  >>> select password from user;"
      echo "  ", packet1

      let (streaming2, packet2) = await await stream
      check streaming2 == false
    waitFor1 sendComQuery() 

  test "query multiply with bad results":
    proc sendComQuery() {.async.} =
      var conn = await open(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      var stream = query(conn, sql("""
select 100;
select password;
select 10;
""", "root"))
      let (streaming0, packet0) = await await stream
      check packet0.kind == rpkResultSet
      echo "  >>> select 100;"
      echo "  ", packet0

      let (streaming1, packet1) = await await stream
      check packet1.kind == rpkError
      echo "  >>> select password from user;"
      echo "  ", packet1

      let (streaming2, packet2) = await await stream
      check streaming2 == false
    waitFor1 sendComQuery() 


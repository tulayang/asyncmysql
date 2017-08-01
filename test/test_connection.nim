#    AsyncMysql - Asynchronous MySQL connector written in pure Nim
#        (c) Copyright 2017 Wang Tong
#
#    See the file "LICENSE", included in this distribution, for
#    details about the copyright.

import unittest, asyncmysql, asyncdispatch, asyncnet, net, strutils

const 
  MysqlHost = "127.0.0.1"
  MysqlPort = Port(3306)
  MysqlUser = "mysql"
  MysqlPassword = "123456"

proc waitFor1(fut: Future[void]) =
  proc check() {.async.} =
    try:
      await fut
    except:
      let err = getCurrentException()
      echo " [", err.name, "] unhandled exception: ", err.msg 
      quit(QuitFailure)
  waitFor check()

proc echoHex(messageHeader: string, s: string) =
  write(stdout, messageHeader)
  for c in s:
    write(stdout, toHex(ord(c), 2), ' ')
  write(stdout, '\L')

suite "Command Queury":
  test "recv handshake initialization packet with 1024 bytes buffer":
    proc sendComQuery() {.async.} =
      var conn = await open(AF_INET, MysqlPort, MysqlHost, MysqlUser, MysqlPassword, "mysql")
      var packet = await query(conn, sql("select host, user from user where user = ?;", "root"))
      echo "  ResultSet Packet: ", packet
      check packet.kind == rpkResultSet
    waitFor1 sendComQuery() 


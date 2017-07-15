#    AsyncMysql - Asynchronous MySQL connector written in pure Nim
#        (c) Copyright 2017 Wang Tong
#
#    See the file "LICENSE", included in this distribution, for
#    details about the copyright.

import unittest, asyncmysql, asyncdispatch, asyncnet, strutils

proc waitFor1(fut: Future[void]) =
  proc check() {.async.} =
    try:
      await fut
    except:
      echo "  !!!FutureError: ", getCurrentExceptionMsg() 
  waitFor check()

suite "Handshake":
  const MysqlHost = "127.0.0.1"
  const MysqlPort = Port(3306)
  const MysqlUser = "mysql"
  const MysqlPassword = "123456"

  var socket = newAsyncSocket(buffered = false) 
  var parser = initGreetingPacketParser() 
  waitFor connect(socket, MysqlHost, MysqlPort)

  test "recv handshake initialization packet":
    proc recvHandshakeInit() {.async.} =
      while true:
        var buf = await recv(socket, 3)
        parse(parser, buf.cstring, buf.len)
        if parser.finished:
          echo "  Buffer length: ", buf.len, " offset: ", parser.offset 
          echo "  Handshake Initialization Packet: ", parser.packet
          check parser.sequenceId == 0
          break
    waitFor1 recvHandshakeInit()  

  test "send client authentication packet":
    proc sendAuth() {.async.} =
      await send(
        socket, 
        toPacketHex(
          (capabilities: 521167,
           maxPacketSize: 0,
           charset: 33,
           user: MysqlUser,
           scrambleBuff: parser.packet.scrambleBuff,
           database: "mysql"), 
        parser.sequenceId + 1, 
        MysqlPassword, 
        true) )
      while true:
        var buf = await recv(socket, 1024)
        check toProtocolInt(buf[3] & "") == parser.sequenceId + 2
        check toProtocolInt(buf[4] & "") == 0
        write(stdout, "  OK Packet: ")
        for c in buf:
          write(stdout, toHex(ord(c), 2), ' ')
        write(stdout, '\L')
        break
    waitFor1 sendAuth()  
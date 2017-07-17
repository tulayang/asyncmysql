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
  var handshakePacket: HandshakePacket
  waitFor connect(socket, MysqlHost, MysqlPort)

  test "recv handshake initialization packet":
    proc recvHandshakeInit() {.async.} =
      var parser = initPacketParser() 
      while true:
        var buf = await recv(socket, 3)
        parse(parser, handshakePacket, buf)
        if parser.finished:
          echo "  Buffer length: ", buf.len, " offset: ", parser.offset 
          echo "  Handshake Initialization Packet: ", handshakePacket
          check handshakePacket.sequenceId == 0
          break
    waitFor1 recvHandshakeInit()  

  test "send client authentication packet":
    proc sendAuth() {.async.} =
      var parser = initPacketParser() 
      var packet: GenericPacket
      await send(
        socket, 
        toPacketHex(
          ClientAuthenticationPacket(
            sequenceId: handshakePacket.sequenceId + 1, 
            capabilities: 521167,
            maxPacketSize: 0,
            charset: 33,
            user: MysqlUser,
            scrambleBuff: handshakePacket.scrambleBuff,
            database: "mysql",
            protocol41: handshakePacket.protocol41), 
        MysqlPassword))
      while true:
        var buf = await recv(socket, 32)
        write(stdout, "  OK Packet: ")
        check toProtocolInt(buf[3] & "") == parser.sequenceId + 2
        check toProtocolInt(buf[4] & "") == 0
        for c in buf:
          write(stdout, toHex(ord(c), 2), ' ')
        write(stdout, '\L')
        parse(parser, packet, handshakePacket, buf)
        if parser.finished:
          echo "  Buffer length: ", buf.len, " offset: ", parser.offset 
          echo "  Handshake Initialization Packet: ", packet
          check parser.sequenceId == 2
          break
    waitFor1 sendAuth()  
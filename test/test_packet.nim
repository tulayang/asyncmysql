#    AsyncMysql - Asynchronous MySQL connector written in pure Nim
#        (c) Copyright 2017 Wang Tong
#
#    See the file "LICENSE", included in this distribution, for
#    details about the copyright.

import unittest, asyncmysql, util, asyncdispatch, asyncnet

const 
  MysqlHost = "127.0.0.1"
  MysqlPort = Port(3306)
  MysqlUser = "mysql"
  MysqlPassword = "123456"

suite "Handshake Authentication With Valid User":
  var socket = newAsyncSocket(buffered = false) 
  var handshakePacket: HandshakePacket
  waitFor1 connect(socket, MysqlHost, MysqlPort)

  test "recv handshake initialization packet with 1024 bytes buffer":
    proc recvHandshakeInitialization() {.async.} =
      var parser = initPacketParser() 
      while true:
        var buf = await recv(socket, 1024)
        parse(parser, handshakePacket, buf)
        if parser.finished:
          echo "  Handshake Initialization Packet: ", handshakePacket
          echo "  Buffer length: 1024, offset: ", parser.offset 
          break
    waitFor1 recvHandshakeInitialization()  

  test "send client authentication packet with valid user":
    proc sendClientAuthentication() {.async.} =
      await send(
        socket, 
        format(
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
    waitFor1 sendClientAuthentication()  

  test "recv result ok packet":
    proc recvResultOk() {.async.} =
      var parser = initPacketParser() 
      var packet: ResultPacket
      while true:
        var buf = await recv(socket, 32)
        check toProtocolInt(buf[3] & "") == parser.sequenceId + 2
        check toProtocolInt(buf[4] & "") == 0
        echoHex "  Result OK Packet: ", buf
        parse(parser, packet, handshakePacket.capabilities, buf)
        if parser.finished:
          echo "  Result Ok Packet: ", packet
          echo "  Buffer length: 32, offset: ", parser.offset 
          check parser.sequenceId == 2
          break
    waitFor1 recvResultOk()  

  close(socket)

suite "Handshake Authentication With Invalid User":
  var socket = newAsyncSocket(buffered = false) 
  var handshakePacket: HandshakePacket
  waitFor connect(socket, MysqlHost, MysqlPort)

  test "recv handshake initialization packet with 3 bytes buffer":
    proc recvHandshakeInitialization() {.async.} =
      var parser = initPacketParser() 
      while true:
        var buf = await recv(socket, 3)
        parse(parser, handshakePacket, buf)
        if parser.finished:
          echo "  Handshake Initialization Packet: ", handshakePacket
          echo "  Buffer length: 3, offset: ", parser.offset 
          check handshakePacket.sequenceId == 0
          break
    waitFor1 recvHandshakeInitialization()  

  test "send client authentication packet with invalid user":
    proc sendClientAuthentication() {.async.} =
      await send(
        socket, 
        format(
          ClientAuthenticationPacket(
            sequenceId: handshakePacket.sequenceId + 1, 
            capabilities: 521167,
            maxPacketSize: 0,
            charset: 33,
            user: "user_name",
            scrambleBuff: handshakePacket.scrambleBuff,
            database: "mysql",
            protocol41: handshakePacket.protocol41), 
        MysqlPassword))
    waitFor1 sendClientAuthentication()

  test "recv result error packet":
    proc recvResultError() {.async.} =
      var parser = initPacketParser() 
      var packet: ResultPacket
      while true:
        var buf = await recv(socket, 16)
        echoHex "  Result Error Packet: ", buf 
        parse(parser, packet, handshakePacket.capabilities, buf)
        if parser.finished:
          echo "  Result Error Packet: ", packet
          echo "  Buffer length: 16, offset: ", parser.offset 
          check parser.sequenceId == 2
          break
    waitFor1 recvResultError()  

  close(socket)


suite "Command Queury":
  var socket = newAsyncSocket(buffered = false) 
  var handshakePacket: HandshakePacket
  waitFor1 connect(socket, MysqlHost, MysqlPort)

  test "recv handshake initialization packet with 1024 bytes buffer":
    proc recvHandshakeInitialization() {.async.} =
      var parser = initPacketParser() 
      while true:
        var buf = await recv(socket, 1024)
        parse(parser, handshakePacket, buf)
        if parser.finished:
          break
    
    proc sendClientAuthentication() {.async.} =
      await send(
        socket, 
        format(
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
    
    proc recvResultOk() {.async.} =
      var parser = initPacketParser(COM_QUERY) 
      var packet: ResultPacket
      while true:
        var buf = await recv(socket, 1024)
        parse(parser, packet, handshakePacket.capabilities, buf)
        if parser.finished:
          check parser.sequenceId == 2
          break

    waitFor1 recvHandshakeInitialization()  
    waitFor1 sendClientAuthentication()  
    waitFor1 recvResultOk()  

  test "ping":
    proc sendComPing() {.async.} =
      await send(socket, formatComPing())
    waitFor1 sendComPing()  

    proc recvResultOk() {.async.} =
      var parser = initPacketParser(COM_PING) 
      var packet: ResultPacket
      while true:
        var buf = await recv(socket, 32)
        echoHex "  Result Ok Packet: ", buf
        parse(parser, packet, handshakePacket.capabilities, buf)
        if parser.finished:
          echo "  Result Ok Packet: ", packet
          check packet.kind == rpkOk
          break
    waitFor1 recvResultOk()  

  test "query `select host, user form user;`":
    proc sendComQuery() {.async.} =
      await send(socket, formatComQuery("SELECT host, user from user;"))
    waitFor1 sendComQuery()  

    proc recvResultSet() {.async.} =
      var parser = initPacketParser(COM_QUERY) 
      var packet: ResultPacket
      while true:
        var buf = await recv(socket, 1024)
        echoHex "  ResultSet Packet: ", buf
        parse(parser, packet, handshakePacket.capabilities, buf.cstring, buf.len)
        if parser.finished:
          echo "  ResultSet Packet: ", packet
          echo "  Buffer length: 1024, offset: ", parser.offset 
          check packet.kind == rpkResultSet
          break
    waitFor1 recvResultSet()  

  test "query `select @@version_comment limit 1;`":
    proc sendComQuery() {.async.} =
      await send(socket, formatComQuery("select @@version_comment limit 1;"))
    waitFor1 sendComQuery()  

    proc recvResultSet() {.async.} =
      var parser = initPacketParser(COM_QUERY) 
      var packet: ResultPacket
      while true:
        var buf = await recv(socket, 3)
        echoHex "  ResultSet Packet: ", buf
        parse(parser, packet, handshakePacket.capabilities, buf.cstring, buf.len)
        if parser.finished:
          echo "  ResultSet Packet: ", packet
          echo "  Buffer length: 3, offset: ", parser.offset 
          check packet.kind == rpkResultSet
          break
    waitFor1 recvResultSet()  

  test "use {database} with `test` database":
    proc sendComInitDb() {.async.} =
      await send(socket, formatComInitDb("test"))
    waitFor1 sendComInitDb()  

    proc recvResultOk() {.async.} =
      var parser = initPacketParser(COM_INIT_DB) 
      var packet: ResultPacket
      while true:
        var buf = await recv(socket, 32)
        echoHex "  Result Ok Packet: ", buf
        parse(parser, packet, handshakePacket.capabilities, buf)
        if parser.finished:
          echo "  Result Ok Packet: ", packet
          check packet.kind == rpkOk
          break
    waitFor1 recvResultOk()  

  test "use {database} with `aaa` (non-existent) database":
    proc sendComInitDb() {.async.} =
      await send(socket, formatComInitDb("aaa"))
    waitFor1 sendComInitDb()  

    proc recvResultError() {.async.} =
      var parser = initPacketParser(COM_INIT_DB) 
      var packet: ResultPacket
      while true:
        var buf = await recv(socket, 32)
        echoHex "  Result Error Packet: ", buf
        parse(parser, packet, handshakePacket.capabilities, buf)
        if parser.finished:
          echo "  Result Error Packet: ", packet
          check packet.kind == rpkError
          break
    waitFor1 recvResultError()  

  test "quit":
    proc sendComQuit() {.async.} =
      await send(socket, formatComQuit())
    waitFor1 sendComQuit()  

    proc recvClosed() {.async.} =
      var parser = initPacketParser(COM_QUIT) 
      var packet: ResultPacket
      while true:
        var buf = await recv(socket, 1024)
        check buf.len == 0
        break
    waitFor1 recvClosed()  

  close(socket)
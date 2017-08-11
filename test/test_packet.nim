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
      var parser = initPacketParser(ppkHandshake) 
      while true:
        let buf = await recv(socket, 1024)
        echoHex "  Handshake Initialization Packet: ", buf
        check buf != ""
        mount(parser, buf.cstring, buf.len)
        let finished = parseHandshake(parser, handshakePacket)
        if finished:
          echo "  Handshake Initialization Packet: ", handshakePacket
          echo "  Buffer length: 1024, offset: ", parser.offset 
          break
    waitFor1 recvHandshakeInitialization()  

  test "send client authentication packet with valid user":
    proc sendClientAuthentication() {.async.} =
      await send(
        socket, 
        formatClientAuth(
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
      var parser = initPacketParser(ppkHandshake) 
      var packet: ResultPacket
      while true:
        let buf = await recv(socket, 32)
        echoHex "  Ok Packet: ", buf
        check buf != ""
        mount(parser, buf.cstring, buf.len)
        let finished = parseResultHeader(parser, packet)
        if finished:
          break
      check packet.kind == rpkOk
      var finished = false
      if parser.buffered:
        finished = parseOk(parser, packet, handshakePacket.capabilities)
      if not finished:
        while true:
          let buf = await recv(socket, 32)
          echoHex "  Ok Packet: ", buf
          check buf != ""
          mount(parser, buf.cstring, buf.len)
          finished = parseOk(parser, packet, handshakePacket.capabilities)
          if finished:
            break
      check finished == true
      echo "  Ok Packet: ", packet
      echo "  Buffer length: 32, offset: ", parser.offset 
      check parser.sequenceId == 2
    waitFor1 recvResultOk()  

  close(socket)

suite "Handshake Authentication With Invalid User":
  var socket = newAsyncSocket(buffered = false) 
  var handshakePacket: HandshakePacket
  waitFor connect(socket, MysqlHost, MysqlPort)

  test "recv handshake initialization packet with 3 bytes buffer":
    proc recvHandshakeInitialization() {.async.} =
      var parser = initPacketParser(ppkHandshake) 
      while true:
        var buf = await recv(socket, 3)
        echoHex "  Handshake Initialization Packet: ", buf
        check buf != ""
        mount(parser, buf.cstring, buf.len)
        let finished = parseHandshake(parser, handshakePacket)
        if finished:
          echo "  Handshake Initialization Packet: ", handshakePacket
          echo "  Buffer length: 3, offset: ", parser.offset 
          check handshakePacket.sequenceId == 0
          break
    waitFor1 recvHandshakeInitialization()  

  test "send client authentication packet with invalid user":
    proc sendClientAuthentication() {.async.} =
      await send(
        socket, 
        formatClientAuth(
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
      var parser = initPacketParser(ppkHandshake) 
      var packet: ResultPacket
      while true:
        let buf = await recv(socket, 16)
        echoHex "  Error Packet: ", buf
        check buf != ""
        mount(parser, buf.cstring, buf.len)
        let finished = parseResultHeader(parser, packet)
        if finished:
          break
      check packet.kind == rpkError
      var finished = false
      if parser.buffered:
        finished = parseError(parser, packet, handshakePacket.capabilities)
      if not finished:
        while true:
          let buf = await recv(socket, 16)
          echoHex "  Error Packet: ", buf
          check buf != ""
          mount(parser, buf.cstring, buf.len)
          finished = parseError(parser, packet, handshakePacket.capabilities)
          if finished:
            break
      check finished == true
      echo "  Error Packet: ", packet
      echo "  Buffer length: 16, offset: ", parser.offset 
      check parser.sequenceId == 2
    waitFor1 recvResultError()  

  close(socket)


suite "Command Queury":
  var socket = newAsyncSocket(buffered = false) 
  var handshakePacket: HandshakePacket
  waitFor1 connect(socket, MysqlHost, MysqlPort)

  test "recv handshake initialization packet":
    proc recvHandshakeInitialization() {.async.} =
      var parser = initPacketParser(ppkHandshake) 
      while true:
        let buf = await recv(socket, 1024)
        check buf != ""
        mount(parser, buf.cstring, buf.len)
        let finished = parseHandshake(parser, handshakePacket)
        if finished:
          break
    
    proc sendClientAuthentication() {.async.} =
      await send(
        socket, 
        formatClientAuth(
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
        let buf = await recv(socket, 1024)
        check buf != ""
        mount(parser, buf.cstring, buf.len)
        let finished = parseResultHeader(parser, packet)
        if finished:
          break
      var finished = false
      if parser.buffered:
        finished = parseOk(parser, packet, handshakePacket.capabilities)
      if not finished:
        while true:
          let buf = await recv(socket, 1024)
          check buf != ""
          mount(parser, buf.cstring, buf.len)
          finished = parseOk(parser, packet, handshakePacket.capabilities)
          if finished:
            break
      check finished == true

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
        let buf = await recv(socket, 1024)
        check buf != ""
        echoHex "  Ok Packet: ", buf
        mount(parser, buf.cstring, buf.len)
        let finished = parseResultHeader(parser, packet)
        if finished:
          break
      check packet.kind == rpkOk
      var finished = false
      if parser.buffered:
        finished = parseOk(parser, packet, handshakePacket.capabilities)
      if not finished:
        while true:
          let buf = await recv(socket, 1024)
          echoHex "  Ok Packet: ", buf
          mount(parser, buf.cstring, buf.len)
          finished = parseOk(parser, packet, handshakePacket.capabilities)
          if finished:
            break
      check finished == true
      echo "  Ok Packet: ", packet
    waitFor1 recvResultOk()  

  test "query `select host, user form user;`":
    proc sendComQuery() {.async.} =
      await send(socket, formatComQuery("SELECT host, user from user;"))
    waitFor1 sendComQuery()  

    proc recvResultSet() {.async.} =
      var parser = initPacketParser(COM_QUERY) 
      var packet: ResultPacket
      while true:
        let buf = await recv(socket, 3)
        check buf != ""
        echoHex "  ResultSet Packet: ", buf
        mount(parser, buf.cstring, buf.len)
        let finished = parseResultHeader(parser, packet)
        if finished:
          break
      check packet.kind == rpkResultSet
      var finished = false
      if parser.buffered:
        finished = parseFields(parser, packet, handshakePacket.capabilities)
      if not finished:
        while true:
          let buf = await recv(socket, 3)
          echoHex "  ResultSet Packet: ", buf
          mount(parser, buf.cstring, buf.len)
          finished = parseFields(parser, packet, handshakePacket.capabilities)
          if finished:
            break
      check finished == true
      check packet.hasRows == true
      if packet.hasRows:
        var rows: seq[string] = @[]
        var rowsPos = -1
        var fieldBuf = newString(1024)
        var offset = 0
        var rowState = rowFieldBegin
        if parser.buffered:
          while true:
            (offset, rowState) = parseRows(parser, packet, handshakePacket.capabilities, fieldBuf.cstring, 1024)
            case rowState
            of rowFieldBegin:
              inc(rowsPos)
              add(rows, "")
            of rowFieldBufFull:
              check offset == 1024
              add(rows[rowsPos], fieldBuf[0..offset-1])
            of rowFieldEnd:
              check offset > 0
              add(rows[rowsPos], fieldBuf[0..offset-1])
            of rowBufEmpty:
              if offset > 0:
                add(rows[rowsPos], fieldBuf[0..offset-1])
              break
            of rowFinished:
              break
        if rowState != rowFinished:
          block parsing:
            while true:
              let buf = await recv(socket, 3)
              echoHex "  ResultSet Packet: ", buf
              mount(parser, buf.cstring, buf.len)
              while true:
                (offset, rowState) = parseRows(parser, packet, handshakePacket.capabilities, fieldBuf.cstring, 1024)
                case rowState 
                of rowFieldBegin:
                  inc(rowsPos)
                  add(rows, "")
                of rowFieldBufFull:
                  check offset == 1024
                  add(rows[rowsPos], fieldBuf[0..offset-1])
                of rowFieldEnd:
                  check offset > 0
                  add(rows[rowsPos], fieldBuf[0..offset-1])
                of rowBufEmpty:
                  if offset > 0:
                    add(rows[rowsPos], fieldBuf[0..offset-1])
                  break
                of rowFinished:
                  break parsing
        echo "  ResultSet Packet: ", packet
        echo "  ResultSet Rows: ", rows
    waitFor1 recvResultSet()  

  test "query `select @@version_comment limit 1;`":
    proc sendComQuery() {.async.} =
      await send(socket, formatComQuery("select @@version_comment limit 1;"))
    waitFor1 sendComQuery()  

    proc recvResultSet() {.async.} =
      var parser = initPacketParser(COM_QUERY) 
      var packet: ResultPacket
      while true:
        let buf = await recv(socket, 3)
        check buf != ""
        echoHex "  ResultSet Packet: ", buf
        mount(parser, buf.cstring, buf.len)
        let finished = parseResultHeader(parser, packet)
        if finished:
          break
      check packet.kind == rpkResultSet
      var finished = false
      if parser.buffered:
        finished = parseFields(parser, packet, handshakePacket.capabilities)
      if not finished:
        while true:
          let buf = await recv(socket, 3)
          echoHex "  ResultSet Packet: ", buf
          mount(parser, buf.cstring, buf.len)
          finished = parseFields(parser, packet, handshakePacket.capabilities)
          if finished:
            break
      check finished == true
      check packet.hasRows == true
      if packet.hasRows:
        var finished = false
        var rows = initRowList()
        if parser.buffered:
          finished = parseRows(parser, packet, handshakePacket.capabilities, rows)
        if not finished:
          while true:
            let buf = await recv(socket, 3)
            echoHex "  ResultSet Packet: ", buf
            mount(parser, buf.cstring, buf.len)
            finished = parseRows(parser, packet, handshakePacket.capabilities, rows)
            if finished:
              break
        echo "  ResultSet Packet: ", packet
        echo "  ResultSet Rows: ", rows.value
    waitFor1 recvResultSet()  

  test "use {database} with `test` database":
    proc sendComInitDb() {.async.} =
      await send(socket, formatComInitDb("test"))
    waitFor1 sendComInitDb()  

    proc recvResultOk() {.async.} =
      var parser = initPacketParser(COM_PING) 
      var packet: ResultPacket
      while true:
        let buf = await recv(socket, 32)
        check buf != ""
        echoHex "  Ok Packet: ", buf
        mount(parser, buf.cstring, buf.len)
        let finished = parseResultHeader(parser, packet)
        if finished:
          break
      check packet.kind == rpkOk
      var finished = false
      if parser.buffered:
        finished = parseOk(parser, packet, handshakePacket.capabilities)
      if not finished:
        while true:
          let buf = await recv(socket, 32)
          echoHex "  Ok Packet: ", buf
          mount(parser, buf.cstring, buf.len)
          finished = parseOk(parser, packet, handshakePacket.capabilities)
          if finished:
            break
      check finished == true
      echo "  Ok Packet: ", packet
    waitFor1 recvResultOk()  

  test "use {database} with `aaa` (non-existent) database":
    proc sendComInitDb() {.async.} =
      await send(socket, formatComInitDb("aaa"))
    waitFor1 sendComInitDb()  

    proc recvResultError() {.async.} =
      var parser = initPacketParser(COM_PING) 
      var packet: ResultPacket
      while true:
        let buf = await recv(socket, 32)
        check buf != ""
        echoHex "  Error Packet: ", buf
        mount(parser, buf.cstring, buf.len)
        let finished = parseResultHeader(parser, packet)
        if finished:
          break
      check packet.kind == rpkError
      var finished = false
      if parser.buffered:
        finished = parseError(parser, packet, handshakePacket.capabilities)
      if not finished:
        while true:
          let buf = await recv(socket, 32)
          echoHex "  Error Packet: ", buf
          mount(parser, buf.cstring, buf.len)
          finished = parseError(parser, packet, handshakePacket.capabilities)
          if finished:
            break
      check finished == true
      echo "  Error Packet: ", packet
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
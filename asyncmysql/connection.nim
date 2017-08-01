#    AsyncMysql - Asynchronous MySQL connector written in pure Nim
#        (c) Copyright 2017 Wang Tong
#
#    See the file "LICENSE", included in this distribution, for
#    details about the copyright.

import asyncdispatch, asyncnet, net, packet, error, query

const 
  MysqlBufSize* = 1024 
  DefaultClientCharset* = CHARSET_UTF8_GENERAL_CI
  DefaultClientCapabilities* = 
    CLIENT_LONG_PASSWORD    or  # Use the improved version of Old Password Authentication.
    CLIENT_FOUND_ROWS       or  # Send found rows instead of affected rows.
    CLIENT_LONG_FLAG        or  # Get all column flags. Longer flags in Protocol::ColumnDefinition320.
    CLIENT_CONNECT_WITH_DB  or  # Database (schema) name can be specified on connect in Handshake Response Packet.
    CLIENT_ODBC             or  # Special handling of ODBC behavior.
    CLIENT_LOCAL_FILES      or  # Can use LOAD DATA LOCAL.
    CLIENT_IGNORE_SPACE     or  # Ignore spaces before '('.
    CLIENT_PROTOCOL_41      or  # Uses the 4.1 protocol.
    CLIENT_IGNORE_SIGPIPE   or  # Do not issue SIGPIPE if network failures occur. 
    CLIENT_TRANSACTIONS     or  # Client knows about transactions.
    CLIENT_RESERVED         or  # DEPRECATED: Old flag for 4.1 protocol.
    CLIENT_RESERVED2        or  # DEPRECATED: Old flag for 4.1 authentication.
    CLIENT_PS_MULTI_RESULTS or  # Multi-results and OUT parameters in PS-protocol.
    CLIENT_MULTI_RESULTS        # Enable multi-results for COM_QUERY.

type
  AsyncMysqlConnection* = ref object
    socket: AsyncSocket
    parser: PacketParser
    handshakePacket: HandshakePacket
    buf: array[MysqlBufSize, char]

proc open*(
  domain: Domain = AF_INET, 
  port = Port(3306), 
  host = "127.0.0.1",
  user: string,
  password: string,
  database: string,
  charset = DefaultClientCharset,
  capabilities = DefaultClientCapabilities
): Future[AsyncMysqlConnection] {.async.} =
  # Opens a database connection.
  new(result)
  result.socket = newAsyncSocket(domain, SOCK_STREAM, IPPROTO_TCP, false)
  await connect(result.socket, host, port)
  result.parser = initPacketParser()
  while true:
    var n = await recvInto(result.socket, result.buf[0].addr, MysqlBufSize)
    if n == 0:
      raiseMysqlError("peer disconnected unexpectedly")
    parse(result.parser, result.handshakePacket, result.buf[0].addr, MysqlBufSize)
    if result.parser.finished:
      break
  await send(
    result.socket, 
    format(
      ClientAuthenticationPacket(
        sequenceId: result.handshakePacket.sequenceId + 1, 
        capabilities: capabilities, # 521167
        maxPacketSize: 0,
        charset: int(charset),
        user: user,
        scrambleBuff: result.handshakePacket.scrambleBuff,
        database: database,
        protocol41: result.handshakePacket.protocol41), 
    password))
  result.parser = initPacketParser()
  var packet: ResultPacket
  while true:
    var n = await recvInto(result.socket, result.buf[0].addr, MysqlBufSize)
    if n == 0:
      raiseMysqlError("peer disconnected unexpectedly")
    parse(result.parser, packet, result.handshakePacket.capabilities, result.buf[0].addr, MysqlBufSize)
    if result.parser.finished:
      break
  if packet.kind == rpkError:
    raiseMysqlError(packet.errorMessage)

proc close*(conn: AsyncMysqlConnection) =
  # Closes the database connection ``conn``.
  close(conn.socket)

proc query*(conn: AsyncMysqlConnection, q: SqlQuery): Future[ResultPacket] {.async.} =
  await send(conn.socket, formatComQuery(string(q)))
  var parser = initPacketParser() 
  while true:
    var n = await recvInto(conn.socket, conn.buf[0].addr, MysqlBufSize)
    if n == 0:
      raiseMysqlError("peer disconnected unexpectedly")
    parse(parser, result, conn.handshakePacket.capabilities, conn.buf[0].addr, MysqlBufSize)
    if parser.finished:
      break    









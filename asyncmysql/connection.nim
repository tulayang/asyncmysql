#    AsyncMysql - Asynchronous MySQL connector written in pure Nim
#        (c) Copyright 2017 Wang Tong
#
#    See the file "LICENSE", included in this distribution, for
#    details about the copyright.

import asyncdispatch, asyncnet, net, packet, error, query, strutils

const 
  MysqlBufSize* = 4096 
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
    CLIENT_MULTI_RESULTS    or  # Enable multi-results for COM_QUERY.
    CLIENT_MULTI_STATEMENTS 

type
  AsyncMysqlConnection* = ref object
    socket: AsyncSocket
    parser: PacketParser
    handshakePacket: HandshakePacket
    buf: array[MysqlBufSize, char]
    bufPos: int
    bufLen: int

  QueryStream* = ref object
    conn: AsyncMysqlConnection
    finished: bool

proc recv(conn: AsyncMysqlConnection): Future[void] {.async.} =
  if conn.bufPos == MysqlBufSize:
    conn.bufPos = 0
    assert conn.bufLen == 0
  if conn.bufLen <= 0:
    assert conn.bufPos < MysqlBufSize
    conn.bufLen = await recvInto(conn.socket, conn.buf[conn.bufPos].addr, MysqlBufSize - conn.bufPos)
    if conn.bufLen == 0:
      raiseMysqlError("peer disconnected unexpectedly")

proc recvResultPacket(conn: AsyncMysqlConnection): Future[ResultPacket] {.async.} =
  conn.parser = initPacketParser() 
  while true:
    await recv(conn)
    parse(conn.parser, result, conn.handshakePacket.capabilities, conn.buf[conn.bufPos].addr, conn.bufLen)
    inc(conn.bufPos, conn.parser.offset)
    dec(conn.bufLen, conn.parser.offset)
    if conn.parser.finished:
      break    

proc handshake(
  conn: AsyncMysqlConnection, 
  domain: Domain, 
  port: Port, 
  host: string,
  user: string,
  password: string,
  database: string,
  charset: int,
  capabilities: int
): Future[void] {.async.} =
  await connect(conn.socket, host, port)
  conn.parser = initPacketParser()
  while true:
    await recv(conn)
    parse(conn.parser, conn.handshakePacket, conn.buf[conn.bufPos].addr, MysqlBufSize)
    inc(conn.bufPos, conn.parser.offset)
    dec(conn.bufLen, conn.parser.offset)
    if conn.parser.finished:
      break
  await send(
    conn.socket, 
    format(
      ClientAuthenticationPacket(
        sequenceId: conn.handshakePacket.sequenceId + 1, 
        capabilities: capabilities, # 521167
        maxPacketSize: 0,
        charset: int(charset),
        user: user,
        scrambleBuff: conn.handshakePacket.scrambleBuff,
        database: database,
        protocol41: conn.handshakePacket.protocol41), 
    password))
  var packet = await recvResultPacket(conn)
  if packet.kind == rpkError:
    raiseMysqlError(packet.errorMessage)

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
  # Opens a new database connection.
  new(result)
  result.socket = newAsyncSocket(domain, SOCK_STREAM, IPPROTO_TCP, false)
  result.bufPos = 0
  result.bufLen = 0
  try:
    await handshake(result, domain, port, host, user, password, database, charset, capabilities)
  except:
    close(result.socket)
    raise getCurrentException()

proc close*(conn: AsyncMysqlConnection) =
  # Closes the database connection ``conn``.
  close(conn.socket)

proc newQueryStream(conn: AsyncMysqlConnection): QueryStream =
  new(result)
  result.conn = conn
  result.finished = false

proc read*(stream: QueryStream): Future[ResultPacket] {.async.} =
  ## Reads a packet from ``stream`` step by step.
  template conn: untyped = stream.conn
  if stream.finished:
    return
  else:
    result = await recvResultPacket(conn)
    if not result.hasMoreResults:
      stream.finished = true

proc finished*(stream: QueryStream): bool =
  result = stream.finished
  
proc execQuery*(conn: AsyncMysqlConnection, q: SqlQuery): Future[QueryStream] {.async.} =
  ## Executes the SQL statements. 
  await send(conn.socket, formatComQuery(string(q)))
  result = newQueryStream(conn)
  
proc execQueryOne*(conn: AsyncMysqlConnection, q: SqlQuery): Future[ResultPacket] {.async.} =
  ## Executes the SQL statement. ``q`` should be a single statement.
  await send(conn.socket, formatComQuery(string(q)))
  result = await recvResultPacket(conn)    

proc execQuit*(conn: AsyncMysqlConnection): Future[void] {.async.} =
  ## Notifies the mysql server that the connection is disconnected. Attempting to request
  ## the server will causes unknown errors.
  await send(conn.socket, formatComQuit())

proc execInitDb*(conn: AsyncMysqlConnection, dbname: string): Future[ResultPacket] {.async.} =
  ## Changes the default schema of the connection.
  await send(conn.socket, formatComInitDb(dbname))
  result = await recvResultPacket(conn)    

proc execPing*(conn: AsyncMysqlConnection): Future[ResultPacket] {.async.} =
  ## Checks whether the connection to the server is working. 
  await send(conn.socket, formatComPing())
  result = await recvResultPacket(conn)    
#    AsyncMysql - Asynchronous MySQL connector written in pure Nim
#        (c) Copyright 2017 Wang Tong
#
#    See the file "LICENSE", included in this distribution, for
#    details about the copyright.

import asyncdispatch, asyncnet, net, packet, error, query, strutils

const 
  MysqlBufSize* = 1024 ## Size of the internal buffer used by mysql connection.
  DefaultClientCharset* = CHARSET_UTF8_GENERAL_CI ## Default charset for mysql connection.
  DefaultClientCapabilities* =  ## Default capabilities for mysql connection.
    CLIENT_LONG_PASSWORD    or  ## Use the improved version of Old Password Authentication.
    CLIENT_FOUND_ROWS       or  ## Send found rows instead of affected rows.
    CLIENT_LONG_FLAG        or  ## Get all column flags. Longer flags in Protocol::ColumnDefinition320.
    CLIENT_CONNECT_WITH_DB  or  ## Database (schema) name can be specified on connect in Handshake Response Packet.
    CLIENT_ODBC             or  ## Special handling of ODBC behavior.
    CLIENT_LOCAL_FILES      or  ## Can use LOAD DATA LOCAL.
    CLIENT_IGNORE_SPACE     or  ## Ignore spaces before '('.
    CLIENT_PROTOCOL_41      or  ## Uses the 4.1 protocol.
    CLIENT_IGNORE_SIGPIPE   or  ## Do not issue SIGPIPE if network failures occur. 
    CLIENT_TRANSACTIONS     or  ## Client knows about transactions.
    CLIENT_RESERVED         or  ## DEPRECATED: Old flag for 4.1 protocol.
    CLIENT_RESERVED2        or  ## DEPRECATED: Old flag for 4.1 authentication.
    CLIENT_PS_MULTI_RESULTS or  ## Multi-results and OUT parameters in PS-protocol.
    CLIENT_MULTI_RESULTS    or  ## Enable multi-results for COM_QUERY.
    CLIENT_MULTI_STATEMENTS 

type
  AsyncMysqlConnection* = ref object ## Asynchronous mysql connection.
    socket: AsyncSocket
    parser: PacketParser
    handshakePacket: HandshakePacket
    buf: array[MysqlBufSize, char]
    bufPos: int
    bufLen: int

  QueryStream* = ref object ## Stream object for queries with multiple statements .
    conn: AsyncMysqlConnection
    finished: bool

var DEBUG_data = ""
var DEBUG_index = 0
proc DEBUG(conn: AsyncMysqlConnection) =
  for c in conn.bufPos..<conn.bufPos+conn.bufLen:
    DEBUG_data.add(conn.buf[c].ord.toHex(2))
    DEBUG_data.add("  ")
    inc(DEBUG_index)
    if DEBUG_index == 10:
      DEBUG_data.add('\L')
      DEBUG_index = 0
  writeFile("/home/king/App/app/asyncmysql/test/log", DEBUG_data)  
  DEBUG_data = ""

proc recv(conn: AsyncMysqlConnection): Future[void] {.async.} =
  if conn.bufPos == MysqlBufSize:
    conn.bufPos = 0
    assert conn.bufLen == 0
  if conn.bufLen <= 0:
    assert conn.bufPos < MysqlBufSize
    conn.bufLen = await recvInto(conn.socket, conn.buf[conn.bufPos].addr, MysqlBufSize - conn.bufPos)
    #DEBUG(conn)
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

proc recvResultPacket(conn: AsyncMysqlConnection, command: ServerCommand): Future[ResultPacket] {.async.} =
  conn.parser = initPacketParser(command) 
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
  # Closes the database connection ``conn`` and releases the associated resources.
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
    result = await recvResultPacket(conn, COM_QUERY)
    if not result.hasMoreResults:
      stream.finished = true

proc finished*(stream: QueryStream): bool =
  ## Determines whether ``stream`` has completed.
  result = stream.finished
  
proc execQuery*(conn: AsyncMysqlConnection, q: SqlQuery): Future[QueryStream] {.async.} =
  ## Executes the SQL statements. ``q`` can be a single statement or multiple statements.
  await send(conn.socket, formatComQuery(string(q)))
  result = newQueryStream(conn)
  
proc execQueryOne*(conn: AsyncMysqlConnection, q: SqlQuery): Future[ResultPacket] {.async.} =
  ## Executes the SQL statement. ``q`` should be a single statement.
  await send(conn.socket, formatComQuery(string(q)))
  result = await recvResultPacket(conn, COM_QUERY)    

proc execQuit*(conn: AsyncMysqlConnection): Future[void] {.async.} =
  ## Notifies the mysql server that the connection is disconnected. Attempting to request
  ## the mysql server again will causes unknown errors.
  ##
  ## ``conn`` should then be closed immediately after that.
  await send(conn.socket, formatComQuit())

proc execInitDb*(conn: AsyncMysqlConnection, database: string): Future[ResultPacket] {.async.} =
  ## Changes the default database on the connection. 
  ##
  ## Equivalent to ``use <database>;``
  await send(conn.socket, formatComInitDb(database))
  result = await recvResultPacket(conn, COM_INIT_DB) 

proc execChangeUser*(conn: AsyncMysqlConnection, user: string, password: string, database: string, 
                     charset = DefaultClientCharset): Future[ResultPacket] {.async.} =
  ## Changes the user and causes the database specified by ``database`` to become the default (current) 
  ## database on the connection specified by mysql. In subsequent queries, this database is 
  ## the default for table references that include no explicit database specifier.
  await send(conn.socket, formatComChangeUser(
    ChangeUserPacket(
      sequenceId: 0,
      user: user,
      scrambleBuff: conn.handshakePacket.scrambleBuff,
      database: database,
      charset: charset), password))
  result = await recvResultPacket(conn, COM_INIT_DB) 

proc execPing*(conn: AsyncMysqlConnection): Future[ResultPacket] {.async.} =
  ## Checks whether the connection to the server is working. 
  await send(conn.socket, formatComPing())
  result = await recvResultPacket(conn, COM_PING)    


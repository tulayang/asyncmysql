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
    resultPacket: ResultPacket
    buf: array[MysqlBufSize, char]
    bufPos: int
    bufLen: int

  QueryStream* = ref object ## Stream object for queries with multiple statements .
    conn: AsyncMysqlConnection
    finished: bool

  BigResultStream* = ref object
    conn: AsyncMysqlConnection
    state: BigResultState

  BigResultState* = enum
    bigFieldBegin,
    bigFieldFull,
    bigFieldEnd,
    bigFinished

when defined(asyncmysqlDebug):
  var debug_data = ""
  var debug_index = 0
  proc debugLog(conn: AsyncMysqlConnection) =
    for c in conn.bufPos..<conn.bufPos+conn.bufLen:
      debug_data.add(conn.buf[c].ord.toHex(2))
      debug_data.add("  ")
      inc(debug_index)
      if debug_index == 10:
        debug_data.add('\L')
        debug_index = 0
    writeFile("test/log", debug_data)  
    debug_data = ""
    debug_index = 0

proc recv(conn: AsyncMysqlConnection): Future[void] {.async.} =
  conn.bufPos = 0
  conn.bufLen = await recvInto(conn.socket, conn.buf[0].addr, MysqlBufSize)
  when defined(asyncmysqlDebug):
    debugLog(conn)
  if conn.bufLen == 0:
    raiseMysqlError("peer disconnected unexpectedly")

proc recvResultHeader(conn: AsyncMysqlConnection): Future[void] {.async.} =
  var finished = false
  if conn.bufLen > 0:
    mount(conn.parser, conn.buf[conn.bufPos].addr, conn.bufLen)
    finished = parseResultHeader(conn.parser, conn.resultPacket)
  if not finished:  
    while true:
      await recv(conn)
      mount(conn.parser, conn.buf[conn.bufPos].addr, conn.bufLen)
      finished = parseResultHeader(conn.parser, conn.resultPacket)
      if finished:
        break

proc recvOk(conn: AsyncMysqlConnection): Future[void] {.async.} =
  var finished = false
  if conn.parser.buffered:
    finished = parseOk(conn.parser, conn.resultPacket, conn.handshakePacket.capabilities)
  if not finished:  
    while true:
      await recv(conn)
      mount(conn.parser, conn.buf[conn.bufPos].addr, conn.bufLen)
      finished = parseOk(conn.parser, conn.resultPacket, conn.handshakePacket.capabilities)
      if finished:
        break

proc recvError(conn: AsyncMysqlConnection): Future[void] {.async.} =
  var finished = false
  if conn.parser.buffered:
    finished = parseError(conn.parser, conn.resultPacket, conn.handshakePacket.capabilities)
  if not finished:  
    while true:
      await recv(conn)
      mount(conn.parser, conn.buf[conn.bufPos].addr, conn.bufLen)
      finished = parseError(conn.parser, conn.resultPacket, conn.handshakePacket.capabilities)
      if finished:
        break

proc recvFields(conn: AsyncMysqlConnection): Future[void] {.async.} =
  var finished = false
  if conn.parser.buffered:
    finished = parseFields(conn.parser, conn.resultPacket, conn.handshakePacket.capabilities)
  if not finished:  
    while true:
      await recv(conn)
      mount(conn.parser, conn.buf[conn.bufPos].addr, conn.bufLen)
      finished = parseFields(conn.parser, conn.resultPacket, conn.handshakePacket.capabilities)
      if finished:
        break

proc recvRows(conn: AsyncMysqlConnection): Future[seq[string]] {.async.} =
  assert conn.resultPacket.hasRows == true
  var rows = initRowList()
  var finished = false
  if conn.parser.buffered:
    finished = parseRows(conn.parser, conn.resultPacket, conn.handshakePacket.capabilities, rows)
  if not finished:  
    while true:
      await recv(conn)
      mount(conn.parser, conn.buf[conn.bufPos].addr, conn.bufLen)
      finished = parseRows(conn.parser, conn.resultPacket, conn.handshakePacket.capabilities, rows)
      if finished:
        break
  shallowCopy(result, rows.value)

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
  conn.parser = initPacketParser(ppkHandshake)
  while true:
    await recv(conn)
    mount(conn.parser, conn.buf[conn.bufPos].addr, conn.bufLen)
    let finished = parseHandshake(conn.parser, conn.handshakePacket)
    if finished:
      break
  inc(conn.bufPos, conn.parser.offset)
  dec(conn.bufLen, conn.parser.offset)
  await send(
    conn.socket, 
    formatClientAuth(
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
  conn.parser = initPacketParser(ppkHandshake) 
  await recvResultHeader(conn)
  case conn.resultPacket.kind
  of rpkOk:
    await recvOk(conn)
  of rpkError:
    await recvError(conn)
    raiseMysqlError(conn.resultPacket.errorMessage)
  of rpkResultSet:
    raiseMysqlError("unexpected result packet kind 'rpkResultSet'")
  inc(conn.bufPos, conn.parser.offset)
  dec(conn.bufLen, conn.parser.offset)

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

proc read*(stream: QueryStream): Future[tuple[packet: ResultPacket, rows: seq[string]]] {.async.} =
  ## Reads a packet from ``stream`` step by step.
  template conn: untyped = stream.conn
  if stream.finished:
    return
  else:
    conn.parser = initPacketParser(COM_QUERY) 
    await recvResultHeader(conn)
    case conn.resultPacket.kind
    of rpkOk:
      await recvOk(conn)
    of rpkError:
      await recvError(conn)
    of rpkResultSet:
      await recvFields(conn)
      if conn.resultPacket.hasRows:
        result.rows = await recvRows(conn)
    result.packet = conn.resultPacket # should copy the packet
    inc(conn.bufPos, conn.parser.offset)
    dec(conn.bufLen, conn.parser.offset)
    if not result.packet.hasMoreResults:
      stream.finished = true

proc finished*(stream: QueryStream): bool =
  ## Determines whether ``stream`` has completed.
  result = stream.finished
  
proc newBigResultStream(conn: AsyncMysqlConnection): BigResultStream =
  new(result)
  result.conn = conn
  result.state = bigFieldBegin

proc read*(stream: BigResultStream, buf: pointer, size: int): Future[tuple[offset: int, state: BigResultState]] {.async.} =
  ## Reads rows of a big-one query.
  template conn: untyped = stream.conn
  if stream.state == bigFinished:
    return (0, bigFinished)
  else:
    var bufPos = 0  
    if conn.parser.buffered:
      let (offset, state) = parseRows(conn.parser, conn.resultPacket, conn.handshakePacket.capabilities, buf, size)
      if state != rowsBufEmpty:
        case state
        of rowsFieldBegin:
          result.state = bigFieldBegin
        of rowsFieldFull:
          result.state = bigFieldFull
        of rowsFieldEnd:
          result.state = bigFieldEnd
        of rowsFinished:
          result.state = bigFinished
          inc(conn.bufPos, conn.parser.offset)
          dec(conn.bufLen, conn.parser.offset)
        of rowsBufEmpty:
          discard
        result.offset = offset
        stream.state = result.state
        return
      inc(bufPos, offset)
    while true:
      await recv(conn)
      mount(conn.parser, conn.buf[conn.bufPos].addr, conn.bufLen)
      let (offset, state) = parseRows(conn.parser, conn.resultPacket, conn.handshakePacket.capabilities, 
          cast[pointer](cast[ByteAddress](buf) + bufPos * sizeof(char)), size - bufPos)
      if state != rowsBufEmpty:
        case state
        of rowsFieldBegin:
          result.state = bigFieldBegin
        of rowsFieldFull:
          result.state = bigFieldFull
        of rowsFieldEnd:
          result.state = bigFieldEnd
        of rowsFinished:
          result.state = bigFinished
          inc(conn.bufPos, conn.parser.offset)
          dec(conn.bufLen, conn.parser.offset)
        of rowsBufEmpty:
          discard
        result.offset = offset
        stream.state = result.state
        return
      inc(bufPos, offset)

proc execQuery*(conn: AsyncMysqlConnection, q: SqlQuery): Future[QueryStream] {.async.} =
  ## Executes the SQL statements. ``q`` can be a single statement or multiple statements.
  await send(conn.socket, formatComQuery(string(q)))
  result = newQueryStream(conn)
  
proc execQueryOne*(conn: AsyncMysqlConnection, q: SqlQuery): 
    Future[tuple[packet: ResultPacket, rows: seq[string]]] {.async.} =
  ## Executes the SQL statement. ``q`` should be a single statement.
  await send(conn.socket, formatComQuery(string(q)))
  conn.parser = initPacketParser(COM_QUERY) 
  await recvResultHeader(conn)
  case conn.resultPacket.kind
  of rpkOk:
    await recvOk(conn)
  of rpkError:
    await recvError(conn)
  of rpkResultSet:
    await recvFields(conn)
    if conn.resultPacket.hasRows:
      result.rows = await recvRows(conn)
  result.packet = conn.resultPacket # should copy the packet
  inc(conn.bufPos, conn.parser.offset)
  dec(conn.bufLen, conn.parser.offset)
  if conn.resultPacket.hasMoreResults:
    raiseMysqlError("bad query, only one SQL statement is allowed")

proc execQueryBigOne*(conn: AsyncMysqlConnection, q: SqlQuery): 
    Future[tuple[packet: ResultPacket, stream: BigResultStream]] {.async.} =
  ## Executes the SQL statement. ``q`` should be a single statement.
  await send(conn.socket, formatComQuery(string(q)))
  conn.parser = initPacketParser(COM_QUERY) 
  await recvResultHeader(conn)
  case conn.resultPacket.kind
  of rpkOk:
    await recvOk(conn)
  of rpkError:
    await recvError(conn)
  of rpkResultSet:
    await recvFields(conn)
    if conn.resultPacket.hasRows:
      result.stream = newBigResultStream(conn)
  result.packet = conn.resultPacket # should copy the packet
  inc(conn.bufPos, conn.parser.offset)
  dec(conn.bufLen, conn.parser.offset) 
  if conn.resultPacket.hasMoreResults:
    raiseMysqlError("bad query, only one SQL statement is allowed")

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
  conn.parser = initPacketParser(COM_INIT_DB) 
  await recvResultHeader(conn)
  case conn.resultPacket.kind
  of rpkOk:
    await recvOk(conn)
  of rpkError:
    await recvError(conn)
  of rpkResultSet:
    raiseMysqlError("unexpected result packet kind 'rpkResultSet'")
  result = conn.resultPacket
  inc(conn.bufPos, conn.parser.offset)
  dec(conn.bufLen, conn.parser.offset)

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
  conn.parser = initPacketParser(COM_CHANGE_USER) 
  await recvResultHeader(conn)
  case conn.resultPacket.kind
  of rpkOk:
    await recvOk(conn)
  of rpkError:
    await recvError(conn)
  of rpkResultSet:
    raiseMysqlError("unexpected result packet kind 'rpkResultSet'")
  result = conn.resultPacket
  inc(conn.bufPos, conn.parser.offset)
  dec(conn.bufLen, conn.parser.offset)

proc execPing*(conn: AsyncMysqlConnection): Future[ResultPacket] {.async.} =
  ## Checks whether the connection to the server is working. 
  await send(conn.socket, formatComPing())
  conn.parser = initPacketParser(COM_PING) 
  await recvResultHeader(conn)
  case conn.resultPacket.kind
  of rpkOk:
    await recvOk(conn)
  of rpkError:
    await recvError(conn)
  of rpkResultSet:
    raiseMysqlError("unexpected result packet kind 'rpkResultSet'")
  result = conn.resultPacket  
  inc(conn.bufPos, conn.parser.offset)
  dec(conn.bufLen, conn.parser.offset)


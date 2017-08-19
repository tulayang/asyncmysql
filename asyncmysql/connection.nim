#    AsyncMysql - Asynchronous MySQL connector written in pure Nim
#        (c) Copyright 2017 Wang Tong
#
#    See the file "LICENSE", included in this distribution, for
#    details about the copyright.

import asyncdispatch, asyncnet, net, mysqlparser, error, query, strutils, deques

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

  AsyncReqLimit* = 1024

type
  AsyncMysqlConnection* = ref object ## Asynchronous mysql connection.
    lock: AsyncLock
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

  AsyncLock = object
    reqs: Deque[Future[void]]

proc initAsyncLock(): AsyncLock =
  result.reqs = initDeque[Future[void]]()

proc acquire(lock: var AsyncLock): Future[void] =
  var retFuture = newFuture[void]("acquire")
  result = retFuture
  let count = lock.reqs.len
  if count >= AsyncReqLimit:
    fail(retFuture, newException(MysqlError, "too much requests"))
  else:
    addLast(lock.reqs, retFuture)
    if count == 0:
      complete(retFuture)

proc release(lock: var AsyncLock) =
  let count = lock.reqs.len
  if count > 0:
    let fut = popFirst(lock.reqs)
    if not fut.finished:
      raiseMysqlError("request not finished")
  if count > 1:
    let fut = peekFirst(lock.reqs)
    callSoon(proc () = complete(fut))

proc deinit(lock: var AsyncLock) =
  lock.reqs = initDeque[Future[void]]()

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

template moveBuf(conn: AsyncMysqlConnection) =
  inc(conn.bufPos, conn.parser.offset)
  dec(conn.bufLen, conn.parser.offset)

template asyncRecvResultHeader(conn: AsyncMysqlConnection) =
  var finished = false
  if conn.bufLen > 0:
    mount(conn.parser, conn.buf[conn.bufPos].addr, conn.bufLen)
    finished = parseResultHeader(conn.parser, conn.resultPacket)
  if not finished:  
    while true:
      yield recv(conn)
      mount(conn.parser, conn.buf[conn.bufPos].addr, conn.bufLen)
      finished = parseResultHeader(conn.parser, conn.resultPacket)
      if finished:
        break

template asyncRecvOk(conn: AsyncMysqlConnection) =
  var finished = false
  if conn.parser.buffered:
    finished = parseOk(conn.parser, conn.resultPacket, conn.handshakePacket.capabilities)
  if not finished:  
    while true:
      yield recv(conn)
      mount(conn.parser, conn.buf[conn.bufPos].addr, conn.bufLen)
      finished = parseOk(conn.parser, conn.resultPacket, conn.handshakePacket.capabilities)
      if finished:
        break

template asyncRecvError(conn: AsyncMysqlConnection) =
  var finished = false
  if conn.parser.buffered:
    finished = parseError(conn.parser, conn.resultPacket, conn.handshakePacket.capabilities)
  if not finished:  
    while true:
      yield recv(conn)
      mount(conn.parser, conn.buf[conn.bufPos].addr, conn.bufLen)
      finished = parseError(conn.parser, conn.resultPacket, conn.handshakePacket.capabilities)
      if finished:
        break

template asyncRecvFields(conn: AsyncMysqlConnection) =
  var finished = false
  if conn.parser.buffered:
    finished = parseFields(conn.parser, conn.resultPacket, conn.handshakePacket.capabilities)
  if not finished:  
    while true:
      yield recv(conn)
      mount(conn.parser, conn.buf[conn.bufPos].addr, conn.bufLen)
      finished = parseFields(conn.parser, conn.resultPacket, conn.handshakePacket.capabilities)
      if finished:
        break

template asyncRecvRows(conn: AsyncMysqlConnection, rows: seq[string]) =
  var rowList = initRowList()
  var finished = false
  if conn.parser.buffered:
    finished = parseRows(conn.parser, conn.resultPacket, conn.handshakePacket.capabilities, rowList)
  if not finished:  
    while true:
      yield recv(conn)
      mount(conn.parser, conn.buf[conn.bufPos].addr, conn.bufLen)
      finished = parseRows(conn.parser, conn.resultPacket, conn.handshakePacket.capabilities, rowList)
      if finished:
        break
  shallowCopy(rows, rowList.value)

proc recvHandshakeInit(conn: AsyncMysqlConnection): Future[void] {.async.} =
  conn.parser = initPacketParser(ppkHandshake)
  while true:
    await recv(conn)
    mount(conn.parser, conn.buf[conn.bufPos].addr, conn.bufLen)
    let finished = parseHandshake(conn.parser, conn.handshakePacket)
    if finished:
      break
  moveBuf(conn)

proc recvHandshakeAck(conn: AsyncMysqlConnection): Future[void] {.async.} =
  conn.parser = initPacketParser(ppkHandshake) 
  asyncRecvResultHeader(conn)
  case conn.resultPacket.kind
  of rpkOk:
    asyncRecvOk(conn)
  of rpkError:
    asyncRecvError(conn)
    raiseMysqlError(conn.resultPacket.errorMessage)
  of rpkResultSet:
    raiseMysqlError("unexpected result packet kind 'rpkResultSet'")
  moveBuf(conn)

proc recvResultBase(conn: AsyncMysqlConnection, cmd: ServerCommand): Future[void] {.async.} = 
  conn.parser = initPacketParser(cmd) 
  asyncRecvResultHeader(conn)
  case conn.resultPacket.kind
  of rpkOk:
    asyncRecvOk(conn)
  of rpkError:
    asyncRecvError(conn)
  of rpkResultSet:
    asyncRecvFields(conn)

proc recvResultAck(conn: AsyncMysqlConnection, cmd: ServerCommand): Future[void] {.async.} = 
  conn.parser = initPacketParser(cmd) 
  asyncRecvResultHeader(conn)
  case conn.resultPacket.kind
  of rpkOk:
    asyncRecvOk(conn)
  of rpkError:
    asyncRecvError(conn)
  of rpkResultSet:
    raiseMysqlError("unexpected result packet kind 'rpkResultSet'")
  moveBuf(conn)

proc recvResultRows(conn: AsyncMysqlConnection, cmd: ServerCommand): 
    Future[seq[string]] {.async.} =
  conn.parser = initPacketParser(cmd) 
  asyncRecvResultHeader(conn)
  case conn.resultPacket.kind
  of rpkOk:
    asyncRecvOk(conn)
  of rpkError:
    asyncRecvError(conn)
  of rpkResultSet:
    asyncRecvFields(conn)
    if conn.resultPacket.hasRows:
      asyncRecvRows(conn, result)
  moveBuf(conn)

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
  await recvHandshakeInit(conn)
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
  await recvHandshakeAck(conn)
  
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
  result.lock = initAsyncLock()
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
  deinit(conn.lock)

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
    try:
      result.rows = await recvResultRows(conn, COM_QUERY)
      result.packet = conn.resultPacket       # should copy the packet
      if not result.packet.hasMoreResults:
        stream.finished = true
        release(stream.conn.lock)
    except:
      release(stream.conn.lock)
      raise getCurrentException()

proc finished*(stream: QueryStream): bool =
  ## Determines whether ``stream`` has completed.
  result = stream.finished
  
proc newBigResultStream(conn: AsyncMysqlConnection): BigResultStream =
  new(result)
  result.conn = conn
  result.state = bigFieldBegin

proc doRead(stream: BigResultStream, buf: pointer, size: int): 
    Future[tuple[offset: int, state: BigResultState]] {.async.} =
  template conn: untyped = stream.conn
  var bufPos = 0  
  if conn.parser.buffered:
    let (offset, state) = parseRows(conn.parser, conn.resultPacket, 
        conn.handshakePacket.capabilities, buf, size)
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
        moveBuf(conn)
      of rowsBufEmpty:
        discard
      result.offset = offset
      stream.state = result.state
      return
    inc(bufPos, offset)
  while true:
    await recv(conn)
    mount(conn.parser, conn.buf[conn.bufPos].addr, conn.bufLen)
    let (offset, state) = parseRows(conn.parser, conn.resultPacket, 
        conn.handshakePacket.capabilities, 
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
        moveBuf(conn)
      of rowsBufEmpty:
        discard
      result.offset = offset
      stream.state = result.state
      return
    inc(bufPos, offset)

proc read*(stream: BigResultStream, buf: pointer, size: int): 
    Future[tuple[offset: int, state: BigResultState]] {.async.} =
  ## Reads rows of a big-one query.
  if stream.state == bigFinished:
    return (0, bigFinished)
  else:
    try:
      result = await doRead(stream, buf, size)
      if result.state == bigFinished:
        release(stream.conn.lock)
    except:
      release(stream.conn.lock)
      raise getCurrentException()

proc execQuery*(conn: AsyncMysqlConnection, q: SqlQuery): Future[QueryStream] {.async.} =
  ## Executes the SQL statements. ``q`` can be a single statement or multiple statements.
  await acquire(conn.lock)
  try:
    await send(conn.socket, formatComQuery(string(q)))
    result = newQueryStream(conn)
  except:
    release(conn.lock)
    raise getCurrentException()
  
proc execQueryOne*(conn: AsyncMysqlConnection, q: SqlQuery): 
    Future[tuple[packet: ResultPacket, rows: seq[string]]] {.async.} =
  ## Executes the SQL statement. ``q`` should be a single statement.
  await acquire(conn.lock)
  try:
    await send(conn.socket, formatComQuery(string(q)))
    result.rows = await recvResultRows(conn, COM_QUERY)
    result.packet = conn.resultPacket       # should copy the packet
    if conn.resultPacket.hasMoreResults:
      raiseMysqlError("bad query, only one SQL statement is allowed")
    release(conn.lock)
  except:  
    release(conn.lock)
    raise getCurrentException()

proc execQueryBigOne*(conn: AsyncMysqlConnection, q: SqlQuery): 
    Future[tuple[packet: ResultPacket, stream: BigResultStream]] {.async.} =
  ## Executes the SQL statement. ``q`` should be a single statement.
  await acquire(conn.lock)
  try:
    await send(conn.socket, formatComQuery(string(q)))
    await recvResultBase(conn, COM_QUERY)
    if conn.resultPacket.kind == rpkResultSet and conn.resultPacket.hasRows:
      result.stream = newBigResultStream(conn)
    else:
      moveBuf(conn)
    result.packet = conn.resultPacket # should copy the packet
    if conn.resultPacket.hasMoreResults:
      raiseMysqlError("bad query, only one SQL statement is allowed")
  except:
    release(conn.lock)
    raise getCurrentException()

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
  await acquire(conn.lock)
  try:
    await send(conn.socket, formatComInitDb(database))
    await recvResultAck(conn, COM_INIT_DB)
    if conn.resultPacket.kind == rpkResultSet:
      raiseMysqlError("unexpected result packet kind 'rpkResultSet'")
    result = conn.resultPacket               # should copy the packet
    release(conn.lock)
  except:  
    release(conn.lock)
    raise getCurrentException()

proc execChangeUser*(conn: AsyncMysqlConnection, user: string, password: string, 
    database: string, charset = DefaultClientCharset): Future[ResultPacket] {.async.} =
  ## Changes the user and causes the database specified by ``database`` to become the default (current) 
  ## database on the connection specified by mysql. In subsequent queries, this database is 
  ## the default for table references that include no explicit database specifier.
  await acquire(conn.lock)
  try:
    await send(conn.socket, formatComChangeUser(
      ChangeUserPacket(
        sequenceId: 0,
        user: user,
        scrambleBuff: conn.handshakePacket.scrambleBuff,
        database: database,
        charset: charset), password))
    await recvResultAck(conn, COM_CHANGE_USER)
    result = conn.resultPacket               # should copy the packet
    release(conn.lock)
  except:  
    release(conn.lock)
    raise getCurrentException()

proc execPing*(conn: AsyncMysqlConnection): Future[ResultPacket] {.async.} =
  ## Checks whether the connection to the server is working. 
  await acquire(conn.lock)
  try:
    await send(conn.socket, formatComPing())
    await recvResultAck(conn, COM_PING)
    result = conn.resultPacket               # should copy the packet
    release(conn.lock)
  except:  
    release(conn.lock)
    raise getCurrentException()
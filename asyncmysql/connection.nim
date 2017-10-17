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

type
  RequestLock = Deque[Future[void]]

  AsyncMysqlConnection* = ref object ## Asynchronous mysql connection.
    socket: AsyncSocket
    parser: PacketParser
    handshakePacket: HandshakePacket
    resultPacket: ResultPacket
    buf: array[MysqlBufSize, char]
    bufPos: int
    bufLen: int
    lock: RequestLock
    closed: bool

proc acquire(L: var RequestLock): Future[void] =
  let retFuture = newFuture[void]("RequestLock.acquire")
  result = retFuture
  addLast(L, retFuture)
  if L.len == 1:
    complete(peekFirst(L))

proc release(L: var RequestLock) =
  if L.len > 0:
    let future = popFirst(L)
    assert future.finished
    if L.len > 0:
      let futureNext = peekFirst(L)
      callSoon() do ():
        complete(futureNext)

proc recv(conn: AsyncMysqlConnection): Future[void] {.async.} =
  conn.bufPos = 0
  conn.bufLen = await recvInto(conn.socket, conn.buf[0].addr, MysqlBufSize)
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

template offsetChar(x: pointer, i: int): pointer =
  cast[pointer](cast[ByteAddress](x) + i * sizeof(char))

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
    # recv body then ...

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
        capabilities: capabilities, # test 521167
        maxPacketSize: 0,
        charset: int(charset),
        user: user,
        scrambleBuff: conn.handshakePacket.scrambleBuff,
        database: database,
        protocol41: conn.handshakePacket.protocol41), 
    password))
  await recvHandshakeAck(conn)
  
proc close*(conn: AsyncMysqlConnection) =
  ## Closes the database connection ``conn`` and releases the associated resources.
  if not conn.closed:
    close(conn.socket)
    conn.closed = true

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
  ## Opens a new database connection.
  new(result)
  result.socket = newAsyncSocket(domain, SOCK_STREAM, IPPROTO_TCP, false)
  result.bufPos = 0
  result.bufLen = 0
  result.lock = initDeque[Future[void]](32)
  result.closed = false
  try:
    await acquire(result.lock)
    await handshake(result, domain, port, host, user, password, database, charset, capabilities)
    release(result.lock)
  except:
    release(result.lock)
    close(result)
    raise getCurrentException()

proc execBigQuery*(conn: AsyncMysqlConnection, q: SqlQuery,
                   packetCb: proc (packet: ResultPacket): Future[void] {.closure, gcsafe.} = nil,
                   packetEndCb: proc (): Future[void] {.closure, gcsafe.} = nil, 
                   fieldCb: proc (field: string): Future[void] {.closure, gcsafe.} = nil): Future[void] =
  ## Executes the SQL statements. ``field`` exposed which is a random length.
  ## 
  ## Notes: this proc applies to fields with small size. 
  ## 
  ## - ``packetCb`` - called when a query is beginning.
  ## - ``fieldCb`` - called when a full field is made.
  ## - ``packetEndCb`` - called when a query is finished.
  let retFuture = newFuture[void]("AsyncMysqlConnection.execQuery 0")
  result = retFuture

  proc exec() {.async.} =
    await send(conn.socket, formatComQuery(string(q)))
    while true:
      block query:
        await recvResultBase(conn, COM_QUERY)
        if packetCb != nil:
          await packetCb(conn.resultPacket)

        if conn.resultPacket.kind == rpkResultSet and conn.resultPacket.hasRows:
          var fieldBuf: string
          var fieldLen = 0
          var bufPos = 0

          if conn.parser.buffered:
            while true:
              let (offset, state) = parseRows(conn.parser, conn.resultPacket, 
                                              conn.handshakePacket.capabilities)
              case state
              of rowsFieldBegin:
                fieldLen = lenPasingField(conn.resultPacket)
                fieldBuf = newString(fieldLen)
                allocPasingField(conn.resultPacket, fieldBuf.cstring, fieldLen)
                bufPos = 0
              of rowsFieldFull:
                discard
              of rowsFieldEnd:
                if fieldCb != nil:
                  await fieldCb(fieldBuf)
              of rowsFinished:
                moveBuf(conn)
                if packetEndCb != nil:
                  await packetEndCb()
                if conn.resultPacket.hasMoreResults:
                  break query
                else:
                  return
              of rowsBufEmpty:
                inc(bufPos, offset)
                let d = fieldLen - bufPos
                if d > 0:
                  allocPasingField(conn.resultPacket, offsetChar(fieldBuf.cstring, bufPos), d)
                break

          while true:
            await recv(conn)
            mount(conn.parser, conn.buf[conn.bufPos].addr, conn.bufLen)
            let (offset, state) = parseRows(conn.parser, conn.resultPacket, 
                                            conn.handshakePacket.capabilities)
            case state
            of rowsFieldBegin:
              fieldLen = lenPasingField(conn.resultPacket)
              fieldBuf = newString(fieldLen)
              allocPasingField(conn.resultPacket, fieldBuf.cstring, fieldLen)
              bufPos = 0
            of rowsFieldFull:
              discard
            of rowsFieldEnd:
              if fieldCb != nil:
                await fieldCb(fieldBuf)
            of rowsFinished:
              moveBuf(conn)
              if packetEndCb != nil:
                await packetEndCb()
              if conn.resultPacket.hasMoreResults:
                break query
              else:
                return
            of rowsBufEmpty:
              inc(bufPos, offset)
              let d = fieldLen - bufPos
              if d > 0:
                allocPasingField(conn.resultPacket, offsetChar(fieldBuf.cstring, bufPos), d)
        else:
          moveBuf(conn)
          if packetEndCb != nil:
            await packetEndCb()
          if conn.resultPacket.hasMoreResults:
            break query
          else:
            return

  acquire(conn.lock).callback = proc (lockFuture: Future[void]) =
    assert lockFuture.failed == false
    exec().callback = proc (execFuture: Future[void]) =
      if execFuture.failed:
        fail(retFuture, readError(execFuture))
      else:
        complete(retFuture)
      release(conn.lock)  

proc execBigQuery*(conn: AsyncMysqlConnection, q: SqlQuery, fieldBufferSize: int, 
                   packetCb: proc (packet: ResultPacket): Future[void] {.closure, gcsafe.} = nil,
                   packetEndCb: proc (): Future[void] {.closure, gcsafe.} = nil,
                   fieldCb: proc (field: string): Future[void] {.closure, gcsafe.} = nil,
                   fieldEndCb: proc (): Future[void] {.closure, gcsafe.} = nil): Future[void] =
  ## Executes the SQL statements. This proc is efficient to deal with large fields.
  ## 
  ## - ``packetCb`` - called when a query is beginning.
  ## - ``fieldCb`` - called when a field fill fully the internal buffer.
  ## - ``fieldEndCb`` - called when a full field is made. 
  ## - ``packetEndCb`` - called when a query is finished.
  let retFuture = newFuture[void]("AsyncMysqlConnection.execQuery 1")
  result = retFuture

  proc exec() {.async.} =
    await send(conn.socket, formatComQuery(string(q)))
    var fieldBuf = newString(fieldBufferSize)
    while true:
      block query:
        await recvResultBase(conn, COM_QUERY)
        if packetCb != nil:
          await packetCb(conn.resultPacket)

        if conn.resultPacket.kind == rpkResultSet and conn.resultPacket.hasRows:
          var bufPos = 0

          if conn.parser.buffered:
            while true:
              let (offset, state) = parseRows(conn.parser, conn.resultPacket, 
                                              conn.handshakePacket.capabilities)
              case state
              of rowsFieldBegin:
                allocPasingField(conn.resultPacket, fieldBuf.cstring, fieldBufferSize)
                bufPos = 0
              of rowsFieldFull:
                if fieldCb != nil:
                  setLen(fieldBuf, fieldBufferSize)
                  await fieldCb(fieldBuf)
              of rowsFieldEnd:
                if offset > 0 and fieldCb != nil:
                  setLen(fieldBuf, offset)
                  await fieldCb(fieldBuf)
                if fieldEndCb != nil:
                  await fieldEndCb()
              of rowsFinished:
                moveBuf(conn)
                if packetEndCb != nil:
                  await packetEndCb()
                if conn.resultPacket.hasMoreResults:
                  break query
                else:
                  return
              of rowsBufEmpty:
                inc(bufPos, offset)
                let d = fieldBufferSize - bufPos
                if d > 0:
                  allocPasingField(conn.resultPacket, offsetChar(fieldBuf.cstring, bufPos), d)
                break

          while true:
            await recv(conn)
            mount(conn.parser, conn.buf[conn.bufPos].addr, conn.bufLen)
            let (offset, state) = parseRows(conn.parser, conn.resultPacket, 
                                            conn.handshakePacket.capabilities)
            case state
            of rowsFieldBegin:
              allocPasingField(conn.resultPacket, fieldBuf.cstring, fieldBufferSize)
              bufPos = 0
            of rowsFieldFull:
              if fieldCb != nil:
                setLen(fieldBuf, fieldBufferSize)
                await fieldCb(fieldBuf)
            of rowsFieldEnd:
              if offset > 0 and fieldCb != nil:
                setLen(fieldBuf, offset)
                await fieldCb(fieldBuf)
              if fieldEndCb != nil:
                await fieldEndCb()
            of rowsFinished:
              moveBuf(conn)
              if packetEndCb != nil:
                await packetEndCb()
              if conn.resultPacket.hasMoreResults:
                break query
              else:
                return
            of rowsBufEmpty:
              inc(bufPos, offset)
              let d = fieldBufferSize - bufPos
              if d > 0:
                allocPasingField(conn.resultPacket, offsetChar(fieldBuf.cstring, bufPos), d)
        else:
          moveBuf(conn)
          if packetEndCb != nil:
            await packetEndCb()
          if conn.resultPacket.hasMoreResults:
            break query
          else:
            return

  acquire(conn.lock).callback = proc (lockFuture: Future[void]) =
    assert lockFuture.failed == false
    exec().callback = proc (execFuture: Future[void]) =
      if execFuture.failed:
        fail(retFuture, readError(execFuture))
      else:
        complete(retFuture)
      release(conn.lock)  

proc execQuery*(conn: AsyncMysqlConnection, q: SqlQuery): 
    Future[seq[tuple[packet: ResultPacket, rows: seq[string]]]] =
  ## Executes the SQL statements. 
  type ResultLoad = tuple[packet: ResultPacket, rows: seq[string]]
  let retFuture = newFuture[seq[ResultLoad]]("AsyncMysqlConnection.execQuery 2")
  var rload: seq[ResultLoad] = @[]
  result = retFuture

  proc exec() {.async.} =
    await send(conn.socket, formatComQuery(string(q)))
    while true:
      let rows = await recvResultRows(conn, COM_QUERY)
      add(rload, (conn.resultPacket, rows))
      if not conn.resultPacket.hasMoreResults:
        break

  acquire(conn.lock).callback = proc (lockFuture: Future[void]) =
    assert lockFuture.failed == false
    exec().callback = proc (execFuture: Future[void]) =
      if execFuture.failed:
        fail(retFuture, readError(execFuture))
      else:
        complete(retFuture, rload)
      release(conn.lock)  

proc execQuit*(conn: AsyncMysqlConnection): Future[void] {.async.} =
  ## Notifies the mysql server that the connection is disconnected. Attempting to request
  ## the mysql server again will causes unknown errors.
  ##
  ## ``conn`` should then be closed immediately after that.
  try:
    await acquire(conn.lock)
    await send(conn.socket, formatComQuit())
  finally:
    release(conn.lock)

proc execInitDb*(conn: AsyncMysqlConnection, database: string): Future[ResultPacket] {.async.} =
  ## Changes the default database on the connection. 
  ##
  ## Equivalent to ``use <database>;``
  try:
    await acquire(conn.lock)
    await send(conn.socket, formatComInitDb(database))
    await recvResultAck(conn, COM_INIT_DB)
    if conn.resultPacket.kind == rpkResultSet:
      raiseMysqlError("unexpected result packet kind 'rpkResultSet'")
  finally:
    release(conn.lock)

proc execChangeUser*(conn: AsyncMysqlConnection, user: string, password: string, 
    database: string, charset = DefaultClientCharset): Future[ResultPacket] {.async.} =
  ## Changes the user and causes the database specified by ``database`` to become the default (current) 
  ## database on the connection specified by mysql. In subsequent queries, this database is 
  ## the default for table references that include no explicit database specifier.
  try:
    await acquire(conn.lock)
    await send(conn.socket, formatComChangeUser(
      ChangeUserPacket(
        sequenceId: 0,
        user: user,
        scrambleBuff: conn.handshakePacket.scrambleBuff,
        database: database,
        charset: charset), password))
    await recvResultAck(conn, COM_CHANGE_USER)
  finally:
    release(conn.lock)

proc execPing*(conn: AsyncMysqlConnection): Future[ResultPacket] {.async.} =
  ## Checks whether the connection to the server is working. 
  try:
    await acquire(conn.lock)
    await send(conn.socket, formatComPing())
    await recvResultAck(conn, COM_PING) 
  finally:
    release(conn.lock)
  
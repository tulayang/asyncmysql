#    AsyncMysql - Asynchronous MySQL connector written in pure Nim
#        (c) Copyright 2017 Wang Tong
#
#    See the file "LICENSE", included in this distribution, for
#    details about the copyright.

## This module implements an connection abstraction, which can connect to the MySQL 
## server by MqSQL Client/Server Protocol.

import asyncdispatch, asyncnet, net, mysqlparser, error, query, strutils, deques

# TODO: timeout

const 
  MysqlBufSize* = 1024 ## Size of the internal buffer that is used by mysql connection.
  
  DefaultClientCharset* = CHARSET_UTF8_GENERAL_CI 
    ## Default charset used by MySQL Client/Server Protocol. This is called "collation" in the SQL-level of 
    ## MySQL (like ``CHARSET_UTF8_GENERAL_CI``). 
  
  DefaultClientCapabilities* =  ## Default client capabilities flag bitmask used by MySQL Client/Server Protocol. 
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

template offsetChar(x: pointer, i: int): pointer =
  cast[pointer](cast[ByteAddress](x) + i * sizeof(char))

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

proc closed*(conn: AsyncMysqlConnection): bool = 
  ## Returns whether ``conn`` is closed.
  result = conn.closed

proc close*(conn: AsyncMysqlConnection) =
  ## Closes the database connection ``conn`` and releases the associated resources. When a 
  ## connection is no longer needed, the user should close it.
  if not conn.closed:
    conn.closed = true
    close(conn.socket)

proc openMysqlConnection*(
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
  ##  
  ## When establishing a connection, you can set the following options:
  ## 
  ## - ``domain`` - the protocol family of the underly socket for this connection.
  ## - ``port`` - the port number to connect to.
  ## - ``host`` - the hostname of the database you are connecting to.
  ## - ``user`` - the MySQL user to authenticate as.
  ## - ``password`` - the password of the MySQL user.
  ## - ``database`` - name of the database to use for this connection.
  ## - ``charset`` - the charset for the connection. (Default: ``DefaultClientCharset``). All available
  ##   charset constants are in a sub-module called **charset**.
  ## - ``capabilities`` - the client capabilitis which is a flag bitmask. (Default: ``DefaultClientCapabilities``). And
  ##   this can be used to affect the connection's behavior. All available charset constants are in a sub-module 
  ##   called **capabilities**.
  new(result)
  result.socket = newAsyncSocket(domain, SOCK_STREAM, IPPROTO_TCP, false)
  result.bufPos = 0
  result.bufLen = 0
  result.lock = initDeque[Future[void]](32)
  result.closed = false
  try:
    await acquire(result.lock)
    await connect(result.socket, host, port)
    await recvHandshakeInit(result)
    await send(
      result.socket, 
      formatClientAuth(
        ClientAuthenticationPacket(
          sequenceId: result.handshakePacket.sequenceId + 1, 
          capabilities: capabilities, # test 521167
          maxPacketSize: 0,
          charset: int(charset),
          user: user,
          scrambleBuff: result.handshakePacket.scrambleBuff,
          database: database,
          protocol41: result.handshakePacket.protocol41), 
      password))
    await recvHandshakeAck(result)
  except:
    close(result)
    raise getCurrentException()
  finally:
    release(result.lock)

proc execQuery*(
  conn: AsyncMysqlConnection, 
  q: SqlQuery,
  finishCb: proc (err: ref Exception): Future[void] {.closure, gcsafe.},
  recvPacketCb: proc (packet: ResultPacket): Future[void] {.closure, gcsafe.} = nil,
  recvPacketEndCb: proc (): Future[void] {.closure, gcsafe.} = nil, 
  recvFieldCb: proc (field: string): Future[void] {.closure, gcsafe.} = nil
) =
  ## Executes the SQL statements in ``q``. 
  ## 
  ## This proc is especially useful when dealing with large result sets. The query process 
  ## is made up of many different stages. At each stage, a different callback proc is called:
  ## 
  ## - ``recvPacketCb`` - called when a SQL statement is beginning.
  ## - ``recvFieldCb`` - called when a complete field is made.
  ## - ``recvPacketEndCb`` - called when a SQL statement is finished.
  ## - ``finishCb`` - called when all SQL statements are finished or occur some errors.
  ## 
  ## For example, when the following statements are executed:
  ## 
  ## .. code-block:: nim
  ## 
  ##   select host from user;
  ##   select id from test;
  ##   insert into test (name) values ('name1') where id = 1;
  ## 
  ## the query process is like this:
  ## 
  ##   1. packet stage
  ## 
  ##      receives a result packet from ``select host from user;``, calls ``recvPacketCb``, and the
  ##      ``kind`` field of the argument ``packet`` is set to ``rpkResultSet``.  
  ##   
  ##   2. field stage
  ## 
  ##      receives a field(column) from ``select host from user;``, calls ``recvFieldCb``. Then, receives
  ##      next field(column), calls ``recvFieldCb`` again. 
  ## 
  ##      ``recvFieldCb`` will be called again by again until
  ##      there is no any field from ``select host from user;``.   
  ## 
  ##   3. packet stage
  ## 
  ##      receives a result packet from ``select id from test;``, calls ``recvPacketCb``, and the
  ##      ``kind`` field of the argument ``packet`` is set to ``rpkResultSet``.  
  ## 
  ##   4. field stage
  ## 
  ##      receives a field(column) from ``select id from user;``, calls ``recvFieldCb``. Then, receives
  ##      next field(column), calls ``recvFieldCb`` again. 
  ## 
  ##      ``recvFieldCb`` will be called again by again until
  ##      there is no any field from ``select host from user;``.  
  ## 
  ##   5. packet stage
  ## 
  ##      receives a result packet from ``insert into test (name) values ('name1') where id = 1;``, calls 
  ##      ``recvPacketCb``, and the ``kind`` field of the argument ``packet`` is set to ``rpkOk``.  
  ## 
  ##   6. finished stage
  ## 
  ##      all SQL statements are finished, calls ``finishCb``. 
  ##     
  ##      Notes: if any errors occur in the above steps, calls ``finishCb`` immediately and ignores other callback procs.
  proc exec() {.async.} =
    await send(conn.socket, formatComQuery(string(q)))
    while true:
      block query:
        await recvResultBase(conn, COM_QUERY)
        if recvPacketCb != nil:
          await recvPacketCb(conn.resultPacket)

        if conn.resultPacket.kind == rpkResultSet and conn.resultPacket.hasRows:
          var fieldBuf: string
          var fieldLen = 0
          var bufPos = 0
          var empty = true

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
                if recvFieldCb != nil:
                  await recvFieldCb(fieldBuf)
              of rowsFinished:
                moveBuf(conn)
                if recvPacketEndCb != nil:
                  await recvPacketEndCb()
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
            if empty:
              await recv(conn)
              mount(conn.parser, conn.buf[conn.bufPos].addr, conn.bufLen)
              empty = false
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
              if recvFieldCb != nil:
                await recvFieldCb(fieldBuf)
            of rowsFinished:
              moveBuf(conn)
              if recvPacketEndCb != nil:
                await recvPacketEndCb()
              if conn.resultPacket.hasMoreResults:
                break query
              else:
                return
            of rowsBufEmpty:
              inc(bufPos, offset)
              let d = fieldLen - bufPos
              if d > 0:
                allocPasingField(conn.resultPacket, offsetChar(fieldBuf.cstring, bufPos), d)
              empty = true
        else:
          moveBuf(conn)
          if recvPacketEndCb != nil:
            await recvPacketEndCb()
          if conn.resultPacket.hasMoreResults:
            break query
          else:
            return

  acquire(conn.lock).callback = proc (lockFuture: Future[void]) =
    assert lockFuture.failed == false
    exec().callback = proc (execFuture: Future[void]) =
      release(conn.lock)  
      if finishCb != nil:
        if execFuture.failed:
          asyncCheck finishCb(readError(execFuture))
        else:
          asyncCheck finishCb(nil)

proc execQuery*(
  conn: AsyncMysqlConnection, 
  q: SqlQuery, 
  bufferSize: int,
  finishCb: proc (err: ref Exception): Future[void] {.closure, gcsafe.}, 
  recvPacketCb: proc (packet: ResultPacket): Future[void] {.closure, gcsafe.} = nil,
  recvPacketEndCb: proc (): Future[void] {.closure, gcsafe.} = nil,
  recvFieldCb: proc (buffer: string): Future[void] {.closure, gcsafe.} = nil,
  recvFieldEndCb: proc (): Future[void] {.closure, gcsafe.} = nil
) =
  ## Executes the SQL statements in ``q``. ``bufferSize`` specifies the size of field buffer.
  ## 
  ## This proc is especially useful when dealing with large result sets. The query process 
  ## is made up of many different stages. At each stage, a different callback proc is called:
  ## 
  ## - ``recvPacketCb`` - called when a SQL statement is beginning.
  ## - ``recvFieldCb`` - called when the content of a field fill fully the internal buffer.
  ## - ``recvFieldEndCb`` - called when a complete field is made.
  ## - ``recvPacketEndCb`` - called when a SQL statement is finished.
  ## - ``finishCb`` - called when all SQL statements are finished or an error occurs.
  proc exec() {.async.} =
    await send(conn.socket, formatComQuery(string(q)))
    var fieldBuf = newString(bufferSize)
    while true:
      block query:
        await recvResultBase(conn, COM_QUERY)
        if recvPacketCb != nil:
          await recvPacketCb(conn.resultPacket)

        if conn.resultPacket.kind == rpkResultSet and conn.resultPacket.hasRows:
          var bufPos = 0
          var empty = true

          if conn.parser.buffered:
            while true:
              let (offset, state) = parseRows(conn.parser, conn.resultPacket, 
                                              conn.handshakePacket.capabilities)
              case state
              of rowsFieldBegin:
                allocPasingField(conn.resultPacket, fieldBuf.cstring, bufferSize)
                bufPos = 0
              of rowsFieldFull:
                if recvFieldCb != nil:
                  setLen(fieldBuf, bufferSize)
                  await recvFieldCb(fieldBuf)
              of rowsFieldEnd:
                if offset > 0 and recvFieldCb != nil:
                  setLen(fieldBuf, offset)
                  await recvFieldCb(fieldBuf)
                if recvFieldEndCb != nil:
                  await recvFieldEndCb()
              of rowsFinished:
                moveBuf(conn)
                if recvPacketEndCb != nil:
                  await recvPacketEndCb()
                if conn.resultPacket.hasMoreResults:
                  break query
                else:
                  return
              of rowsBufEmpty:
                inc(bufPos, offset)
                let d = bufferSize - bufPos
                if d > 0:
                  allocPasingField(conn.resultPacket, offsetChar(fieldBuf.cstring, bufPos), d)
                break

          while true:
            if empty:
              await recv(conn)
              mount(conn.parser, conn.buf[conn.bufPos].addr, conn.bufLen)
              empty = false
            let (offset, state) = parseRows(conn.parser, conn.resultPacket, 
                                            conn.handshakePacket.capabilities)
            case state
            of rowsFieldBegin:
              allocPasingField(conn.resultPacket, fieldBuf.cstring, bufferSize)
              bufPos = 0
            of rowsFieldFull:
              if recvFieldCb != nil:
                setLen(fieldBuf, bufferSize)
                await recvFieldCb(fieldBuf)
            of rowsFieldEnd:
              if offset > 0 and recvFieldCb != nil:
                setLen(fieldBuf, offset)
                await recvFieldCb(fieldBuf)
              if recvFieldEndCb != nil:
                await recvFieldEndCb()
            of rowsFinished:
              moveBuf(conn)
              if recvPacketEndCb != nil:
                await recvPacketEndCb()
              if conn.resultPacket.hasMoreResults:
                break query
              else:
                return
            of rowsBufEmpty:
              inc(bufPos, offset)
              let d = bufferSize - bufPos
              if d > 0:
                allocPasingField(conn.resultPacket, offsetChar(fieldBuf.cstring, bufPos), d)
              empty = true
        else:
          moveBuf(conn)
          if recvPacketEndCb != nil:
            await recvPacketEndCb()
          if conn.resultPacket.hasMoreResults:
            break query
          else:
            return

  acquire(conn.lock).callback = proc (lockFuture: Future[void]) =
    assert lockFuture.failed == false
    exec().callback = proc (execFuture: Future[void]) =
      release(conn.lock)  
      if finishCb != nil:
        if execFuture.failed:
          asyncCheck finishCb(readError(execFuture))
        else:
          asyncCheck finishCb(nil)

proc execQuery*(
  conn: AsyncMysqlConnection, 
  q: SqlQuery, 
  finishCb: proc (
    err: ref Exception, 
    replies: seq[tuple[packet: ResultPacket, rows: seq[string]]]
  ): Future[void] {.closure, gcsafe.}
) =
  ## Executes the SQL statements in ``q``. 
  ## 
  ## This proc places all the results in memory. When dealing with large result sets, this can be 
  ## inefficient and take up a lot of memory, so you can try the other two ``execQuery`` procs
  ## at this point. 
  ## 
  ## - ``finishCb`` - called when all SQL statements are finished or an error occurs.
  var replies: seq[tuple[packet: ResultPacket, rows: seq[string]]] = @[]

  proc exec() {.async.} =
    await send(conn.socket, formatComQuery(string(q)))
    while true:
      let rows = await recvResultRows(conn, COM_QUERY)
      add(replies, (conn.resultPacket, rows))
      if not conn.resultPacket.hasMoreResults:
        break

  acquire(conn.lock).callback = proc (lockFuture: Future[void]) =
    assert lockFuture.failed == false
    exec().callback = proc (execFuture: Future[void]) =
      release(conn.lock)  
      if finishCb != nil:
        if execFuture.failed:
          asyncCheck finishCb(readError(execFuture), replies)
        else:
          asyncCheck finishCb(nil, replies)

proc execQuit*(
  conn: AsyncMysqlConnection, 
  finishCb: proc (
    err: ref Exception
  ): Future[void] {.closure, gcsafe.} 
) =
  ## Notifies the mysql server that the connection is disconnected. Attempting to request
  ## the mysql server again will causes unknown errors.
  ## 
  ## - ``finishCb`` - called when this task is finished or an error occurs.
  acquire(conn.lock).callback = proc (lockFuture: Future[void]) =
    assert lockFuture.failed == false
    send(conn.socket, formatComQuit()).callback = proc (execFuture: Future[void]) =
      release(conn.lock)  
      if finishCb != nil:
        if execFuture.failed:
          asyncCheck finishCb(readError(execFuture))
        else:
          asyncCheck finishCb(nil)

proc execInitDb*(
  conn: AsyncMysqlConnection, 
  database: string, 
  finishCb: proc (
    err: ref Exception,
    reply: ResultPacket
  ): Future[void] {.closure, gcsafe.}
) =
  ## Changes the default database on the connection. 
  ##
  ## Equivalent to ``use <database>;`` 
  ## 
  ## - ``finishCb`` - called when this task is finished or an error occurs.
  proc exec() {.async.} =
    await send(conn.socket, formatComInitDb(database))
    await recvResultAck(conn, COM_INIT_DB)
    if conn.resultPacket.kind == rpkResultSet:
      raiseMysqlError("unexpected result packet kind 'rpkResultSet'")

  acquire(conn.lock).callback = proc (lockFuture: Future[void]) =
    assert lockFuture.failed == false
    exec().callback = proc (execFuture: Future[void]) =
      release(conn.lock)  
      if finishCb != nil:
        if execFuture.failed:
          asyncCheck finishCb(readError(execFuture), conn.resultPacket)
        else:
          asyncCheck finishCb(nil, conn.resultPacket)

proc execChangeUser*(
  conn: AsyncMysqlConnection, 
  user: string, 
  password: string, 
  database: string, 
  charset = DefaultClientCharset,
  finishCb: proc (
    err: ref Exception,
    reply: ResultPacket
  ): Future[void] {.closure, gcsafe.}
) =
  ## Changes the user and causes the database specified by ``database`` to become the default (current) 
  ## database on the connection specified by mysql. In subsequent queries, this database is 
  ## the default for table references that include no explicit database specifier.
  ## 
  ## - ``finishCb`` - called when this task is finished or an error occurs.
  proc exec() {.async.} =
    await send(conn.socket, formatComChangeUser(
      ChangeUserPacket(
        sequenceId: 0,
        user: user,
        scrambleBuff: conn.handshakePacket.scrambleBuff,
        database: database,
        charset: charset), password))
    await recvResultAck(conn, COM_CHANGE_USER)

  acquire(conn.lock).callback = proc (lockFuture: Future[void]) =
    assert lockFuture.failed == false
    exec().callback = proc (execFuture: Future[void]) =
      release(conn.lock)  
      if finishCb != nil:
        if execFuture.failed:
          asyncCheck finishCb(readError(execFuture), conn.resultPacket)
        else:
          asyncCheck finishCb(nil, conn.resultPacket)

proc execPing*(
  conn: AsyncMysqlConnection,
  finishCb: proc (
    err: ref Exception,
    reply: ResultPacket
  ): Future[void] {.closure, gcsafe.}
) =
  ## Checks whether the connection to the server is working. 
  proc exec() {.async.} =
    await send(conn.socket, formatComPing())
    await recvResultAck(conn, COM_PING) 

  acquire(conn.lock).callback = proc (lockFuture: Future[void]) =
    assert lockFuture.failed == false
    exec().callback = proc (execFuture: Future[void]) =
      release(conn.lock)  
      if finishCb != nil:
        if execFuture.failed:
          asyncCheck finishCb(readError(execFuture), conn.resultPacket)
        else:
          asyncCheck finishCb(nil, conn.resultPacket)
#    AsyncMysql - Asynchronous MySQL connector written in pure Nim
#        (c) Copyright 2017 Wang Tong
#
#    See the file "LICENSE", included in this distribution, for
#    details about the copyright.

## This module implements an connection pool. The connection pool is very efficient, 
## it automatically assigns connections, and queues when there is no connection, until 
## a connection is available.

import asyncdispatch, asyncnet, net, mysqlparser, error, query, connection, strutils, deques

# TODO: timeout

type
  ConnectionFlag = enum
    connIdle
  
  Config = object
    domain: Domain
    port: Port
    host: string
    user: string
    password: string
    database: string
    charset: int
    capabilities: int
    connectionLimit: int

  AsyncMysqlPool* = ref object ## The connection pool.
    conns: seq[tuple[conn: AsyncMysqlConnection, flags: set[ConnectionFlag]]]
    connFutures: Deque[Future[int]]
    config: Config

  RequestCb* = proc (conn: AsyncMysqlConnection, connIx: int): Future[void] {.closure, gcsafe.}

proc openMysqlPool*(
  domain: Domain = AF_INET, 
  port = Port(3306), 
  host = "127.0.0.1",
  user: string,
  password: string,
  database: string,
  charset = DefaultClientCharset,
  capabilities = DefaultClientCapabilities,
  connectionLimit = 10
): Future[AsyncMysqlPool] {.async.} = 
  ## Creates a new connection pool. 
  ## 
  ## You can set the following options:
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
  ## - ``connectionLimit`` - The maximum number of connections to create at once. 
  ## 
  ## Pool accept all the same options as a connection. When creating a new connection, the options are simply 
  ## passed to the connection.
  new(result)
  result.conns = newSeqOfCap[tuple[conn: AsyncMysqlConnection, flags: set[ConnectionFlag]]](connectionLimit)
  result.connFutures = initDeque[Future[int]](32)
  result.config.domain = domain
  result.config.port = port
  result.config.host = host
  result.config.user = user
  result.config.password = password
  result.config.database = database
  result.config.charset = charset
  result.config.capabilities = capabilities
  result.config.connectionLimit = connectionLimit
  for i in 0..<result.config.connectionLimit:
    try:
      let conn = await openMysqlConnection(domain, port, host, user, password, database, charset, capabilities)
      add(result.conns, (conn, {connIdle}))
    except:
      for j in 0..<i:
        close(result.conns[j].conn)
      setLen(result.conns, 0)
      raise getCurrentException()

proc close*(p: AsyncMysqlPool) =
  ## Closes the pool ``p``, all the connections in the pool will be closed immediately.
  for i in 0..<p.conns.len:
    if p.conns[i].conn != nil:
      close(p.conns[i].conn)
  setLen(p.conns, 0)

proc countAvailableConnections*(p: AsyncMysqlPool): int =
  ## Returns count of the current available connections.
  result = 0
  for i in 0..<p.conns.len:
    if connIdle in p.conns[i].flags and 
       p.conns[i].conn != nil and
       not p.conns[i].conn.closed: 
      inc(result)

proc acquire(p: AsyncMysqlPool): Future[int] = 
  let retFuture = newFuture[int]("AsyncMysqlPool.acquire")
  result = retFuture
  var closedIx = -1
  for i in 0..<p.config.connectionLimit:
    if connIdle in p.conns[i].flags:
      if p.conns[i].conn == nil or p.conns[i].conn.closed:
        closedIx = i
      else:
        excl(p.conns[i].flags, connIdle)
        complete(retFuture, i)
        return 
  if closedIx >= 0:
    openMysqlConnection(
      p.config.domain,   p.config.port,     
      p.config.host,     p.config.user, 
      p.config.password, p.config.database, 
      p.config.charset,  p.config.capabilities
    ).callback = proc (fut: Future[AsyncMysqlConnection]) = 
      if fut.failed:
        p.conns[closedIx].conn = nil
        fail(retFuture, fut.readError)
      else:
        p.conns[closedIx].conn = fut.read
        excl(p.conns[closedIx].flags, connIdle)
        complete(retFuture, closedIx)
  else:
    addLast(p.connFutures, retFuture)

proc release*(p: AsyncMysqlPool, connIx: int) =
  ## Signal that the connection is return to the pool, ready to be used again by someone else.
  ## ``connIx`` is the connection ID in the pool.
  if p.connFutures.len > 0:
    let connFuture = popFirst(p.connFutures)
    callSoon() do ():
      complete(connFuture, connIx)  
  else:
    incl(p.conns[connIx].flags, connIdle)

proc request*(p: AsyncMysqlPool, cb: RequestCb) = 
  ## Requires pool ``p`` to assign a connection. 
  ## 
  ## When an available connection is obtained, the connection is passed to the callback proc, and the 
  ## callback ``cb`` is called.
  ## 
  ## Notes: don't close the connection in ``cb``. When ``cb`` is finished, the connection is automatically 
  ## released to the pool inside, and other requests can reuse this connection.
  ## 
  ## This proc is particularly useful when handling transactions. When there is no error in the previous 
  ## queries then executing **commit**, otherwise **rollbackk** must be excuted to protect the data.
  ## 
  ## .. code-block:: nim
  ## 
  ##   proc execMyRequest(pool: AsyncMysqlPool): Future[void] = 
  ##     var retFuture = newFuture[void]("myRequest")
  ##     result = retFuture
  ##     
  ##     proc execRollback(conn: AsyncMysqlConnection): Future[void] =
  ##       var retFuture = newFuture[void]("execRollback")    
  ##       result = retFuture
  ##
  ##       proc finishCb(
  ##         err: ref Exception, 
  ##         replies: seq[tuple[packet: ResultPacket, rows: seq[string]]]
  ##       ) {.async.} =
  ##         ##     do something if needed ... 
  ##         if err == nil: 
  ##           retFuture.complete()
  ##         else:
  ##           retFuture.fail(err)
  ##
  ##       conn.execQuery(sql("rollback"), finishCb)
  ##
  ##     proc execTransaction(conn: AsyncMysqlConnection): Future[void] = 
  ##       var retFuture = newFuture[void]("execTransaction")
  ##       result = retFuture
  ##      
  ##       proc finishCb(
  ##         err: ref Exception, 
  ##         replies: seq[tuple[packet: ResultPacket, rows: seq[string]]]
  ##       ) {.async.} =
  ##         ##     do something
  ##         if err == nil: 
  ##           if replies.len < 5 or 
  ##             replies[0].packet.kind == rpkError or
  ##             replies[1].packet.kind == rpkError or
  ##             replies[2].packet.kind == rpkError or
  ##             replies[3].packet.kind == rpkError or
  ##             replies[4].packet.kind == rpkError:
  ##             try:
  ##               await conn.execRollback()
  ##             except:
  ##               retFuture.fail(getCurrentException())
  ##           else:
  ##             retFuture.complete()
  ##         else:
  ##           retFuture.fail(err)
  ##
  ##       conn.execQuery(sql("""
  ##         start transaction;
  ##         select val from sample where id = ?;
  ##         update sample set val = 1 where id = ?;
  ##         insert into sample (val) values (200),,,;
  ##         commit;
  ##         """, "1", "1"), finishCb)
  ## 
  ##     proc cb(conn: AsyncMysqlConnection, connIx: int) {.async.} =
  ##       try:
  ##         await conn.execTransaction() 
  ##         pool.release(connIx)
  ##         retFuture.complete()
  ##       except:
  ##         pool.release(connIx) 
  ##         retFuture.fail(getCurrentException())
  ## 
  ##     pool.request(cb)
  ## 
  ##   proc main() {.async.} = 
  ##     await pool.execMyRequest()
  proc task() {.async.} = 
    let connIx = await acquire(p)
    let conn = p.conns[connIx].conn
    await cb(conn, connIx)
  asyncCheck task()  

proc execQuery*(
  p: AsyncMysqlPool,
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
  proc requestCb(conn: AsyncMysqlConnection, connIx: int): Future[void] =
    var retFuture = newFuture[void]("AsyncMysqlPool.execQuery")
    result = retFuture
    proc finishWrapCb(err: ref Exception) {.async.} =
      await finishCb(err)
      if err != nil:
        close(conn)
      release(p, connIx)
      complete(retFuture)
    execQuery(conn, q, finishWrapCb, recvPacketCb, recvPacketEndCb, recvFieldCb)
  request(p, requestCb)

proc execQuery*(
  p: AsyncMysqlPool,
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
  proc requestCb(conn: AsyncMysqlConnection, connIx: int): Future[void] =
    var retFuture = newFuture[void]("AsyncMysqlPool.execQuery")
    result = retFuture
    proc finishWrapCb(err: ref Exception) {.async.} =
      await finishCb(err)
      if err != nil:
        close(conn)
      release(p, connIx)
      complete(retFuture)
    execQuery(conn, q, bufferSize, finishWrapCb, recvPacketCb, recvPacketEndCb, recvFieldCb, recvFieldEndCb)
  request(p, requestCb)

proc execQuery*(
  p: AsyncMysqlPool,
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
  proc requestCb(conn: AsyncMysqlConnection, connIx: int): Future[void] =
    var retFuture = newFuture[void]("AsyncMysqlPool.execQuery")
    result = retFuture
    proc finishWrapCb(
      err: ref Exception, 
      replies: seq[tuple[packet: ResultPacket, rows: seq[string]]]
    ) {.async.} =
      await finishCb(err, replies)
      if err != nil:
        close(conn)
      release(p, connIx)
      complete(retFuture)
    execQuery(conn, q, finishWrapCb)
  request(p, requestCb)

proc execInitDb*(
  p: AsyncMysqlPool,
  database: string, 
  finishCb: proc (
    err: ref Exception,
    reply: ResultPacket
  ): Future[void] {.closure, gcsafe.}
) =
  ## Notifies the mysql server that the connection is disconnected. Attempting to request
  ## the mysql server again will causes unknown errors.
  ## 
  ## - ``finishCb`` - called when this task is finished or an error occurs.
  proc requestCb(conn: AsyncMysqlConnection, connIx: int): Future[void] =
    var retFuture = newFuture[void]("AsyncMysqlPool.execInitDb")
    result = retFuture
    proc finishWrapCb(
      err: ref Exception, 
      reply: ResultPacket
    ) {.async.} =
      await finishCb(err, reply)
      if err != nil:
        close(conn)
      release(p, connIx)
      complete(retFuture)
    execInitDb(conn, database, finishWrapCb)
  request(p, requestCb)

proc execChangeUser*(
  p: AsyncMysqlPool,
  user: string, 
  password: string, 
  database: string, 
  charset = DefaultClientCharset,
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
  proc requestCb(conn: AsyncMysqlConnection, connIx: int): Future[void] =
    var retFuture = newFuture[void]("AsyncMysqlPool.execChangeUser")
    result = retFuture
    proc finishWrapCb(
      err: ref Exception, 
      reply: ResultPacket
    ) {.async.} =
      await finishCb(err, reply)
      if err != nil:
        close(conn)
      release(p, connIx)
      complete(retFuture)
    execChangeUser(conn, user, password, database, charset, finishWrapCb)
  request(p, requestCb)







    
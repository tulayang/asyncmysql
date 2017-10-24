#    AsyncMysql - Asynchronous MySQL connector written in pure Nim
#        (c) Copyright 2017 Wang Tong
#
#    See the file "LICENSE", included in this distribution, for
#    details about the copyright.

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
    capacity: int

  AsyncMysqlPool* = ref object
    conns: seq[tuple[conn: AsyncMysqlConnection, flags: set[ConnectionFlag]]]
    connFutures: Deque[Future[int]]
    config: Config

  RequestCb = proc (conn: AsyncMysqlConnection): Future[void] {.closure, gcsafe.}

proc acquire(p: AsyncMysqlPool): Future[int] = 
  let retFuture = newFuture[int]("AsyncMysqlPool.acquire")
  result = retFuture
  var closedIx = -1
  for i in 0..<p.config.capacity:
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

proc release(p: AsyncMysqlPool, connIx: int) =
  if p.connFutures.len > 0:
    let connFuture = popFirst(p.connFutures)
    callSoon() do ():
      complete(connFuture, connIx)  
  else:
    incl(p.conns[connIx].flags, connIdle)

proc openMysqlPool*(
  domain: Domain = AF_INET, 
  port = Port(3306), 
  host = "127.0.0.1",
  user: string,
  password: string,
  database: string,
  charset = DefaultClientCharset,
  capabilities = DefaultClientCapabilities,
  capacity = 10
): Future[AsyncMysqlPool] {.async.} = 
  new(result)
  result.conns = newSeqOfCap[tuple[conn: AsyncMysqlConnection, flags: set[ConnectionFlag]]](capacity)
  result.connFutures = initDeque[Future[int]](32)
  result.config.domain = domain
  result.config.port = port
  result.config.host = host
  result.config.user = user
  result.config.password = password
  result.config.database = database
  result.config.charset = charset
  result.config.capabilities = capabilities
  result.config.capacity = capacity
  for i in 0..<result.config.capacity:
    try:
      let conn = await openMysqlConnection(domain, port, host, user, password, database, charset, capabilities)
      add(result.conns, (conn, {connIdle}))
    except:
      for j in 0..<i:
        close(result.conns[j].conn)
      setLen(result.conns, 0)
      raise getCurrentException()

proc close*(p: AsyncMysqlPool) =
  for i in 0..<p.conns.len:
    if p.conns[i].conn != nil:
      close(p.conns[i].conn)
  setLen(p.conns, 0)

proc countAvailableConnections*(p: AsyncMysqlPool): int =
  result = 0
  for i in 0..<p.conns.len:
    if connIdle in p.conns[i].flags and 
       p.conns[i].conn != nil and
       not p.conns[i].conn.closed: 
      inc(result)

proc request*(p: AsyncMysqlPool, cb: RequestCb) = 
  proc task() {.async.} = 
    let connIx = await acquire(p)
    let conn = p.conns[connIx].conn
    await cb(conn)
    release(p, connIx)
  asyncCheck task()  

proc execQuery*(
  p: AsyncMysqlPool,
  q: SqlQuery,
  finishCb: proc (err: ref Exception): Future[void] {.closure, gcsafe.},
  recvPacketCb: proc (packet: ResultPacket): Future[void] {.closure, gcsafe.} = nil,
  recvPacketEndCb: proc (): Future[void] {.closure, gcsafe.} = nil, 
  recvFieldCb: proc (field: string): Future[void] {.closure, gcsafe.} = nil
) =
  proc requestCb(conn: AsyncMysqlConnection): Future[void] =
    var retFuture = newFuture[void]("AsyncMysqlPool.execQuery")
    result = retFuture
    proc finishWrapCb(err: ref Exception) {.async.} =
      await finishCb(err)
      if err != nil:
        close(conn)
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
  proc requestCb(conn: AsyncMysqlConnection): Future[void] =
    var retFuture = newFuture[void]("AsyncMysqlPool.execQuery")
    result = retFuture
    proc finishWrapCb(err: ref Exception) {.async.} =
      await finishCb(err)
      if err != nil:
        close(conn)
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
  proc requestCb(conn: AsyncMysqlConnection): Future[void] =
    var retFuture = newFuture[void]("AsyncMysqlPool.execQuery")
    result = retFuture
    proc finishWrapCb(
      err: ref Exception, 
      replies: seq[tuple[packet: ResultPacket, rows: seq[string]]]
    ) {.async.} =
      await finishCb(err, replies)
      if err != nil:
        close(conn)
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
  proc requestCb(conn: AsyncMysqlConnection): Future[void] =
    var retFuture = newFuture[void]("AsyncMysqlPool.execInitDb")
    result = retFuture
    proc finishWrapCb(
      err: ref Exception, 
      reply: ResultPacket
    ) {.async.} =
      await finishCb(err, reply)
      if err != nil:
        close(conn)
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
  proc requestCb(conn: AsyncMysqlConnection): Future[void] =
    var retFuture = newFuture[void]("AsyncMysqlPool.execChangeUser")
    result = retFuture
    proc finishWrapCb(
      err: ref Exception, 
      reply: ResultPacket
    ) {.async.} =
      await finishCb(err, reply)
      if err != nil:
        close(conn)
      complete(retFuture)
    execChangeUser(conn, user, password, database, charset, finishWrapCb)
  request(p, requestCb)







    
#    AsyncMysql - Asynchronous MySQL connector written in pure Nim
#        (c) Copyright 2017 Wang Tong
#
#    See the file "LICENSE", included in this distribution, for
#    details about the copyright.

import asyncdispatch, asyncnet, net, mysqlparser, error, query, connection, strutils, deques

type
  ConnectionFlag = enum
    connConnected, connIdle
  
  Config = object
    domain: Domain
    port: Port
    host: string
    user: string
    password: string
    database: string
    charset: int
    capabilities: int
    connCapacity: int

  AsyncMysqlPool* = ref object
    conns: seq[tuple[conn: AsyncMysqlConnection, flags: set[ConnectionFlag]]]
    connFutures: Deque[Future[int]]
    config: Config

  RequestCb = proc (conn: AsyncMysqlConnection): Future[void] {.closure, gcsafe.}

proc acquire(p: AsyncMysqlPool): Future[int] = 
  let retFuture = newFuture[int]("AsyncMysqlPool.acquire")
  result = retFuture
  var discIx = -1
  for i in 0..<p.config.connCapacity:
    if connIdle in p.conns[i].flags:
      if connConnected in p.conns[i].flags:
        assert p.conns[i].conn != nil
        excl(p.conns[i].flags, connIdle)
        complete(retFuture, i)
        return 
      else:
        discIx = i
  if discIx >= 0:
    open(p.config.domain,   p.config.port,     
         p.config.host,     p.config.user, 
         p.config.password, p.config.database, 
         p.config.charset,  p.config.capabilities).callback = proc (fut: Future[AsyncMysqlConnection]) = 
      if fut.failed:
        p.conns[discIx].conn = nil
        fail(retFuture, fut.readError)
      else:
        p.conns[discIx].conn = fut.read
        excl(p.conns[discIx].flags, connIdle)
        incl(p.conns[discIx].flags, connConnected)
        complete(retFuture, discIx)
  else:
    addLast(p.connFutures, retFuture)

proc release(p: AsyncMysqlPool, connIx: int) =
  if p.connFutures.len > 0:
    let connFuture = popFirst(p.connFutures)
    callSoon() do ():
      complete(connFuture, connIx)  
  else:
    incl(p.conns[connIx].flags, connIdle)

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
      complete(retFuture)
    execChangeUser(conn, user, password, database, charset, finishWrapCb)
  request(p, requestCb)







    
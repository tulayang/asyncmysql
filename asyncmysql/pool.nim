#    AsyncMysql - Asynchronous MySQL connector written in pure Nim
#        (c) Copyright 2017 Wang Tong
#
#    See the file "LICENSE", included in this distribution, for
#    details about the copyright.

import asyncdispatch, asyncnet, net, mysqlparser, error, query, strutils, deques

type
  ConnectionFlag = enum
    connConnected, connIdle

  AsyncMysqlPool* = ref object
    conns: seq[AsyncMysqlConnection]
    connsLen: int
    connFutures: Deque[Future[AsyncMysqlConnection]]

  RequestCb = proc (conn: AsyncMysqlConnection): Future[void] {.closure, gcsafe.}

proc getConnection(p: AsyncMysqlPool): Future[int] = 
  let retFuture = newFuture[void]("AsyncMysqlPool.getConnection")
  result = retFuture
  var discIx = -1
  for i in 0..<p.connsLen:
    if connIdle in p.conns[i].flags:
      if connConnected in p.conns[i].flags:
        excl(p.conns[i].flags, connIdle)
        complete(retFuture, i)
        return 
      else:
        discIx = i
  if discIx >= 0:
    # p.conns[discIx] = await open()
    # complete(retFuture, discIx)
  else:
    addLast(p.connFutures, retFuture)

proc release(p: AsyncMysqlPool, connIx: int) =
  if p.connFutures.len > 0:
    let connFuture = popFirst(p.connFutures)
    callSoon() do ():
      if connConnected notin conn.flags:
        open().callback = proc (fut: Future[void]) =
          if fut.failed:
            fail(connFuture, readError(fut))
            release(p, connIx)
          else:
            p.conns[connIx] = read(fut)
            complete(connFuture, p.conns[connIx])
      else:
        complete(connFuture, p.conns[connIx])  
  else:
    incl(p.conns[connIx].flags, connIdle)

proc request*(p: AsyncMysqlPool, cb: RequestCb): Future[void] {.async.} = 
  ## 这是一个原子操作，绑定一个专用的连接，查询，然后释放这个连接。当 cb 完成的时候，这个连接会在内部自动释放
  let connIx = await getConnection(p)
  await cb(p.conns[connIx])
  release(p, connIx)
  
proc execQuery(p: AsyncMysqlPool): Future[void] =
  let retFuture = newFuture[void]("AsyncMysqlPool.execQuery")
  result = retFuture
  request(p) do (conn: AsyncMysqlConnection) {.async.}:
    complete(retFuture)
    
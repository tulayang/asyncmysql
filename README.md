# AsyncMysql [![Build Status](https://travis-ci.org/tulayang/asyncmysql.svg?branch=master)](https://travis-ci.org/tulayang/asyncmysql)

Currently, the packet parser has been completed. The ``PacketParser`` is an excellent incremental parser which is not limited by the size of the buffer. For example, using a buffer of 16 bytes buffer to request the mysql server:

```nim
var socket = newAsyncSocket(buffered = false) 
var handshakePacket: HandshakePacket

proc recvHandshakeInitialization() {.async.} =
  var parser = initPacketParser() 
  while true:
    var buf = await socket.recv(16)
    parser.parse(handshakePacket, buf)
    if parser.finished:
      break

proc sendClientAuthentication() {.async.} =
  await socket.send(
    format(
      ClientAuthenticationPacket(
        sequenceId: handshakePacket.sequenceId + 1, 
        capabilities: 521167,
        maxPacketSize: 0,
        charset: 33,
        user: MysqlUser,
        scrambleBuff: handshakePacket.scrambleBuff,
        database: "mysql",
        protocol41: handshakePacket.protocol41), 
    MysqlPassword))

proc recvResultOk() {.async.} =
  var parser = initPacketParser() 
  var packet: ResultPacket
  while true:
    var buf = await socket.recv(16)
    parser.parse(packet, handshakePacket.capabilities, buf)
    if parser.finished:
      break

proc sendComQuery() {.async.} =
  await socket.send(formatComQuery("SELECT host, user from user;"))

proc recvResultSet() {.async.} =
  var parser = initPacketParser() 
  var packet: ResultPacket
  while true:
    var buf = await socket.recv(16)
    parser.parse(packet, handshakePacket.capabilities, buf.cstring, buf.len)
    if parser.finished:
      break

proc main() {.async.} =
  await connect(socket, "127.0.0.1", Port(3306))
  await recvHandshakeInitialization()  
  await sendClientAuthentication() 
  await recvResultOk()
  await sendComQuery() 
  await recvResultSet()  

waitFor main()
```



TODO: building connection and query tools and connection pools.


```nim
var conn = newAsyncMysqlConnection("127.0.0.1", Port(3306))
let ret = await conn.query(sql"""
  start transaction;
  select * from table1 where id = ?;
  insert into table2 (name) values (?);
  commit;
""", 100, "Twitter")
assert ret[0].kind == rpkOk
assert ret[1].kind == rpkResultSet
assert ret[2].kind == rpkOk
assert ret[3].kind == rpkOk
```

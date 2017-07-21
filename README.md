# AsyncMysql [![Build Status](https://travis-ci.org/tulayang/asyncmysql.svg?branch=master)](https://travis-ci.org/tulayang/asyncmysql)

Currently, the packet parser has been completed. The ``PacketParser`` is an excellent incremental parser which is not limited by the size of the buffer. For example, using a buffer of 3 bytes buffer to request the mysql server:

```nim
var socket = newAsyncSocket(buffered = false) 
var handshakePacket: HandshakePacket

proc recvHandshakeInitialization() {.async.} =
  var parser = initPacketParser() 
  while true:
    var buf = await recv(socket, 1024)
    parse(parser, handshakePacket, buf)
    if parser.finished:
      break

proc sendClientAuthentication() {.async.} =
  await send(
    socket, 
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
    var buf = await recv(socket, 1024)
    parse(parser, packet, handshakePacket.capabilities, buf)
    if parser.finished:
      check parser.sequenceId == 2
      break

proc sendComQuery() {.async.} =
  await send(socket, formatComQuery("SELECT host, user from user;"))

proc recvResultSet() {.async.} =
  var parser = initPacketParser() 
  var packet: ResultPacket
  while true:
    var buf = await recv(socket, 1024)
    echoHex "  ResultSet Packet: ", buf
    parse(parser, packet, handshakePacket.capabilities, buf.cstring, buf.len)
    if parser.finished:
      echo "  ResultSet Packet: ", packet
      echo "  Buffer length: 1024, offset: ", parser.offset 
      check packet.kind == rpkResultSet
      break

proc main() {.async.} =
  await connect(socket, "127.0.0.1", Port(3306))
  await recvHandshakeInitialization()  
  await sendClientAuthentication()  
  await recvResultOk()  

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

#    AsyncMysql - Asynchronous MySQL connector written in pure Nim
#        (c) Copyright 2017 Wang Tong
#
#    See the file "LICENSE", included in this distribution, for
#    details about the copyright.

#[
3              packet Length 
1              packet sequenceId
1              [0a] protocolVersion serverVersion
string[NUL]    server serverVersion
4              connection id
string[8]      scramble buff 1
1              [00] filler
2              capability flags (lower 2 bytes)
1              character set
2              serverStatus flags
  if capabilities & CLIENT_PROTOCOL_41 {
2              capability flags (upper 2 bytes)
1              scramble payloadLength
10             reserved (all [00])
string[12]     scramble buff 2
1              [00] filler
  } else {
13             [00] filler
  }
  if more data in the packet {
string[NUL]    auth-plugin name  
  }
]#

import strutils, securehash, math

const
  ## Values for the capabilities flag bitmask used by Client/Server Protocol.
  ## Currently need to fit into 32 bits.
  ## Each bit represents an optional feature of the protocol.
  ## Both the client and the server are sending these.
  ## The intersection of the two determines whast optional parts of the protocol will be used.
  CLIENT_LONG_PASSWORD* = 1
  CLIENT_FOUND_ROWS* = 1 shl 1 
  CLIENT_LONG_FLAG* = 1 shl 2 
  CLIENT_CONNECT_WITH_DB* = 1 shl 3 
  CLIENT_NO_SCHEMA* = 1 shl 4 
  CLIENT_COMPRESS* = 1 shl 5
  CLIENT_ODBC* = 1 shl 6 
  CLIENT_LOCAL_FILES* = 1 shl 7 
  CLIENT_IGNORE_SPACE* = 1 shl 8
  CLIENT_PROTOCOL_41* = 1 shl 9 
  CLIENT_INTERACTIVE* = 1 shl 10 
  CLIENT_SSL* = 1 shl 11
  CLIENT_IGNORE_SIGPIPE* = 1 shl 12 
  CLIENT_TRANSACTIONS* = 1 shl 13 
  CLIENT_RESERVED* = 1 shl 14
  CLIENT_RESERVED2* = 1 shl 15 
  CLIENT_MULTI_STATEMENTS* = 1 shl 16 
  CLIENT_MULTI_RESULTS* = 1 shl 17 
  CLIENT_PS_MULTI_RESULTS* = 1 shl 18 
  CLIENT_PLUGIN_AUTH * = 1 shl 19
  CLIENT_CONNECT_ATTRS* = 1 shl 20 
  CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA* = 1 shl 21
  CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS* = 1 shl 22 
  CLIENT_SESSION_TRACK* = 1 shl 23
  CLIENT_DEPRECATE_EOF* = 1 shl 24 
  CLIENT_SSL_VERIFY_SERVER_CERT* = 1 shl 30
  CLIENT_REMEMBER_OPTIONS* = 1 shl 31

proc toProtocolHex*(x: Natural, len: Positive): string =
  ## `(0xFAFF, 2)` => `"\xFF\xFA"` 
  var n = x
  result = newString(len)
  for i in 0..<int(len):
    result[i] = chr(n and 0xFF)
    n = n shr 8

proc toProtocolInt*(str: string): Natural =
  ## `"\xFF\xFA"` => `0xFAFF`  
  result = 0
  var i = 0
  for c in str:
    inc(result, ord(c) shl (8 * i)) # c.int * pow(16.0, i.float32 * 2).int
    inc(i)

template offsetChar*(x: pointer, i: int): pointer =
  cast[pointer](cast[ByteAddress](x) + i * sizeof(char))

template offsetCharVal*(x: pointer, i: int): char =
  cast[ptr char](offsetChar(x, i))[]

proc joinFixedStr(s: var string, want: var int, buf: pointer, size: int) =
  # Parses only one packet buf not the whole buf.
  let n = if size > want: want else: size
  for i in 0..<n:
    s.add(offsetCharVal(buf, i)) 
  dec(want, n)

proc joinNulStr(s: var string, buf: pointer, size: int): tuple[finished: bool, count: int] =
  # Parses only one packet buf not the whole buf.
  result.finished = false
  for i in 0..<size:
    inc(result.count)
    if offsetCharVal(buf, i) == '\0':
      result.finished = true
      return
    else:
      s.add(offsetCharVal(buf, i))

type
  PacketHeaderState* = enum
    phsPayloadLength, phsSequenceId, phsFinished

  PacketHeaderParser* = object
    payloadLength*: int
    sequenceId*: int
    offset*: int
    packetOffset*: int
    ended*: bool
    state: PacketHeaderState

proc initPacketHeaderParser*(): PacketHeaderParser = 
  discard

proc clear*(p: var PacketHeaderParser) =
  p.packetOffset = 0
  p.ended = true
  p.state = phsPayloadLength

proc parse*(p: var PacketHeaderParser, word: var string, want: var int, buf: pointer, size: int) = 
  var pos = 0
  while true:
    case p.state
    of phsPayloadLength:
      let w = want
      assert pos == 0
      joinFixedStr(word, want, buf, size)
      inc(pos, w - want)
      if want > 0: 
        p.offset = pos
        return 
      p.payloadLength = toProtocolInt(word)
      if p.payloadLength == 0xFFFFFF:
        p.ended = false
      elif p.payloadLength == 0:
        p.ended = true
      p.state = phsSequenceId
      word = ""
      want = 1
    of phsSequenceId:
      let w = want
      joinFixedStr(word, want, offsetChar(buf, size - pos), size - pos)
      inc(pos, w - want)
      if want > 0:
        p.offset = pos
        return
      p.sequenceId = toProtocolInt(word)
      p.state = phsFinished
      word = ""
      inc(p.packetOffset)
    of phsFinished:
      p.offset = pos
      return

proc finished*(p: PacketHeaderParser): bool =
  result = p.state == phsFinished

type
  GreetingPacket* = tuple       
    ## Packet from mysql server when connecting to the server that requires authentication.
    protocolVersion: int      # 1
    serverVersion: string     # NullTerminatedString
    threadId: int             # 4
    scrambleBuff1: string     # 8
    capabilities: int         # (4)
    capabilities1: int        # 2
    charset: int              # 1
    serverStatus: int         # 2
    capabilities2: int        # [2]
    scrambleLen: int          # [1]
    scrambleBuff2: string     # [12]
    scrambleBuff: string      # 8 + [12]
    plugin: string            # NullTerminatedString                               

  GreetingPacketState* = enum
    gpsPacketHeader, gpsExtraPacketHeader, gpsProtocolVersion, gpsServerVersion, gpsThreadId,  
    gpsScrambleBuff1,gpsFiller0, gpsCapabilities1, gpsCharSet, gpsStatus, gpsCapabilities2, 
    gpsFiller1, gpsFiller2, gpsScrambleBuff2, gpsFiller3, gpsPlugin, gpsFinished

  GreetingPacketParser* = object ## Packet p for authentication.
    hparser: PacketHeaderParser
    packet*: GreetingPacket
    word: string
    want: int
    regWant: int
    offset: int
    wantPacketLen: int
    state: GreetingPacketState
    regState: GreetingPacketState

proc initGreetingPacketParser*(): GreetingPacketParser =
  ## Initialize a new ``GreetingPacketParser``.
  result.hparser = initPacketHeaderParser()
  clear(result.hparser)
  result.packet.serverVersion = ""
  result.packet.scrambleBuff1 = ""
  result.packet.scrambleBuff2 = ""
  result.packet.plugin = "" 
  result.word = ""
  result.want = 3
  result.regWant = 3
  result.state = gpsPacketHeader
  result.regState = gpsPacketHeader

proc parse*(parser: var GreetingPacketParser, buf: pointer, size: int) =
  template hparser: untyped = parser.hparser
  template packet: untyped = parser.packet

  template next() =
    clear(parser.hparser)
    parser.regState = parser.state
    parser.regWant = parser.want
    parser.state = gpsExtraPacketHeader
    parser.want = 3  
    continue

  template checkIfNext() =
    assert realSize == 0
    if size > pos:
      assert parser.wantPacketLen == 0
      if hparser.ended:
        raise newException(ValueError, "invalid packet")
      else:
        next
    else: 
      if parser.wantPacketLen > 0:
        parser.offset = pos
        return
      else: # == 0
        if hparser.ended:
          raise newException(ValueError, "invalid packet")
        else:
          next

  template parseFixed(field: var int) =
    let want = parser.want
    joinFixedStr(parser.word, parser.want, offsetChar(buf, pos), realSize)
    let n = want - parser.want
    inc(pos, n)
    dec(realSize, n)
    dec(parser.wantPacketLen, n)
    if parser.want > 0:
      checkIfNext
    else: 
      field = toProtocolInt(parser.word)
      parser.word = ""

  template parseFixed(field: var string) =
    let want = parser.want
    joinFixedStr(field, parser.want, offsetChar(buf, pos), realSize)
    let n = want - parser.want
    inc(pos, n)
    dec(realSize, n)
    dec(parser.wantPacketLen, n)
    if parser.want > 0:
      checkIfNext

  template parseNul(field: var string) {.dirty.} =
    let (finished, count) = joinNulStr(field, offsetChar(buf, pos), realSize)
    inc(pos, count)
    dec(realSize, count)
    dec(parser.wantPacketLen, count)
    if not finished:
      checkIfNext

  template parseFiller() {.dirty.} =
    if parser.want > realSize:
      inc(pos, realSize)
      dec(parser.wantPacketLen, realSize)
      dec(parser.want, realSize)
      dec(realSize, realSize)
      checkIfNext
    else:  
      let n = parser.want
      inc(pos, n)
      dec(realSize, n)
      dec(parser.wantPacketLen, n)
      dec(parser.want, n)

  var pos = 0
  var realSize: int

  if parser.state != gpsPacketHeader and parser.state != gpsExtraPacketHeader:
    realSize = if parser.wantPacketLen <= size: parser.wantPacketLen
               else: size

  while true:
    case parser.state
    of gpsPacketHeader:
      parse(hparser, parser.word, parser.want, buf, size)
      inc(pos, hparser.offset)
      if not hparser.finished:
        assert pos == size
        parser.offset = pos
        return 
      parser.wantPacketLen = hparser.payloadLength
      realSize = if size - pos > parser.wantPacketLen: parser.wantPacketLen
                 else: size - pos 
      parser.state = gpsProtocolVersion
      parser.want = 1  
    of gpsExtraPacketHeader:
      assert parser.wantPacketLen == 0
      parse(hparser, parser.word, parser.want, offsetChar(buf, pos), size - pos)
      inc(pos, hparser.offset)
      if not hparser.finished:
        assert pos == size
        parser.offset = pos
        return 
      parser.wantPacketLen = hparser.payloadLength
      realSize = if size - pos > parser.wantPacketLen: parser.wantPacketLen
                 else: size - pos
      parser.state = parser.regState
      parser.want = parser.regWant
      parser.regState = gpsPacketHeader  
    of gpsProtocolVersion:
      parseFixed(packet.protocolVersion)
      parser.state = gpsServerVersion
    of gpsServerVersion:
      parseNul(packet.serverVersion)
      parser.state = gpsThreadId
      parser.want = 4
    of gpsThreadId:
      parseFixed(packet.threadId)
      parser.state = gpsScrambleBuff1
      parser.want = 8
    of gpsScrambleBuff1:
      parseFixed(packet.scrambleBuff1)
      parser.state = gpsFiller0
      parser.want = 1
    of gpsFiller0:
      parseFiller()
      parser.state = gpsCapabilities1
      parser.want = 2
    of gpsCapabilities1:
      parseFixed(packet.capabilities1)
      parser.state = gpsCharSet
      parser.want = 1
    of gpsCharSet:
      parseFixed(packet.charset)
      parser.state = gpsStatus
      parser.want = 2
    of gpsStatus:
      parseFixed(packet.serverStatus)
      if (packet.capabilities1 and CLIENT_PROTOCOL_41) > 0:
        parser.state = gpsCapabilities2
        parser.want = 2
      else:
        parser.state = gpsFiller3
        parser.want = 13
    of gpsCapabilities2:
      parseFixed(packet.capabilities2)
      packet.capabilities = packet.capabilities1 + 16 * packet.capabilities2
      parser.state = gpsFiller1
      parser.want = 1
    of gpsFiller1:
      parseFixed(packet.scrambleLen)
      parser.state = gpsFiller2
      parser.want = 10
    of gpsFiller2:
      parseFiller()
      parser.state = gpsScrambleBuff2
      # scrambleBuff2 should be 0x00 terminated, but sphinx does not do this
      # so we assume scrambleBuff2 to be 12 byte and treat the next byte as a
      # filler byte.
      parser.want = 12
    of gpsScrambleBuff2:
      parseFixed(packet.scrambleBuff2)
      packet.scrambleBuff = packet.scrambleBuff1 & packet.scrambleBuff2
      parser.state = gpsFiller3
      parser.want = 1
    of gpsFiller3:
      parseFiller()
      if hparser.ended and parser.wantPacketLen == 0:
        parser.state = gpsFinished
      else:  
        parser.state = gpsPlugin
    of gpsPlugin:
      # According to the docs this should be 0x00 terminated, but MariaDB does
      # not do this, so we assume this string to be packet terminated.
      parseNul(packet.plugin)
      parser.state = gpsFinished
    of gpsFinished:
      parser.offset = pos
      return

proc parse*(parser: var GreetingPacketParser, buf: string) =
  ## Parse the ``buf`` data.
  parse(parser, buf.cstring, buf.len)

proc finished*(parser: GreetingPacketParser): bool =
  ## Is ``parser`` finished successful ?
  result = parser.state == gpsFinished

proc sequenceId*(parser: GreetingPacketParser): int = 
  result = parser.hparser.sequenceId

proc offset*(parser: GreetingPacketParser): int = 
  result = parser.offset

type
  ClientAuthenticationPacket* = tuple 
    ## Packet for login request.
    capabilities: int         # 4
    maxPacketSize: int        # 4
    charset: int              # [1]
    # filler: string          # [23]
    user: string              # NullTerminatedString
    # scrambleLen             # 1
    scrambleBuff: string      # 20
    database: string          # NullTerminatedString

proc parseHex(c: char): int =
  case c
  of '0'..'9':
    result = ord(c.toUpperAscii) - ord('0') 
  of 'a'..'f':
    result = ord(c.toUpperAscii) - ord('A') + 10
  of 'A'..'F':
    result = ord(c.toUpperAscii) - ord('A') + 10
  else:
    raise newException(ValueError, "invalid hex char: " & c)

proc `xor`(a: string, b: string): string =
  assert a.len == b.len
  result = newStringOfCap(a.len)
  for i in 0..<a.len:
    let c = ord(a[i]) xor ord(b[i])
    add(result, chr(c))

proc sha1(seed: string): string =
  const len = 20
  result = newString(len)
  let s = $secureHash(seed) # TODO: optimize
  for i in 0..<len:
    result[i] = chr(parseHex(s[i*2]) shl 4 + parseHex(s[i*2+1]))

proc token(scrambleBuff: string, password: string): string =
  let stage1 = sha1(password)
  let stage2 = sha1(stage1)
  let stage3 = sha1(scrambleBuff & stage2)
  result = stage3 xor stage1

proc hash323(s: string): tuple[a: uint32, b: uint32] =
  # TODO: type unit optimize, make more safety
  var nr = 0x50305735'u32
  var add = 7'u32
  var nr2 = 0x12345671'u32
  var tmp: uint32
  for c in s:
    case c
    of '\x09', '\x20':
      continue
    else:
      tmp = 0xFF and ord(c)
      nr = nr xor ((((nr and 63) + add) * tmp) + (nr shl 8))
      nr2 = nr2 + ((nr2 shl 8) xor nr)
      add = add + tmp
  result.a = nr and 0x7FFFFFFF
  result.b = (nr2 and 0x7FFFFFFF)

proc scramble323(seed: string, password: string): string =
  # TODO: type unit optimize, make more safety
  assert password != nil
  if password == "":
    return ""
  var pw = hash323(seed)
  var msg = hash323(password)
  const max = 0x3FFFFFFF'u32
  var seed1 = (pw.a xor msg.a) mod max
  var seed2 = (pw.b xor msg.b) mod max
  var b: uint32
  result = newString(seed.len)
  for i in 0..<seed.len:
    seed1 = ((seed1 * 3) + seed2) mod max
    seed2 = (seed1 + seed2 + 33) mod max
    b = floor((seed1.int / max.int * 31) + 64).uint32
    result[i] = chr(b)
  seed1 = ((seed1 * 3) + seed2) mod max
  seed2 = (seed1 + seed2 + 33) mod max
  b = floor(seed1.int / max.int * 31).uint32
  for i in 0..<seed.len:
    result[i] = chr(ord(result[i]) xor b.int)

proc toPacketHex*(packet: ClientAuthenticationPacket, sequenceId: int,
                  password: string, protocol41: bool): string =
  var payloadLength: int
  if protocol41:
    payloadLength = 4 + 4 + 1 + 23 + packet.user.len + 1 + 1 +
                    20 + packet.database.len + 1
    result = newStringOfCap(4 + payloadLength)
    add(result, toProtocolHex(payloadLength, 3))
    add(result, toProtocolHex(sequenceId, 1))
    add(result, toProtocolHex(packet.capabilities, 4))
    add(result, toProtocolHex(packet.maxPacketSize, 4))
    add(result, toProtocolHex(packet.charset, 1))
    add(result, toProtocolHex(0, 23))
    add(result, packet.user)
    add(result, '\0')
    add(result, toProtocolHex(20, 1))
    add(result, token(packet.scrambleBuff, password))
    add(result, packet.database)
    add(result, '\0')
  else:
    payloadLength = 2 + 3 + packet.user.len + 1 + 
                    8 + 1 + packet.database.len + 1
    result = newStringOfCap(4 + payloadLength)                
    add(result, toProtocolHex(payloadLength, 3))
    add(result, toProtocolHex(sequenceId, 1))

    add(result, toProtocolHex(packet.capabilities, 2))
    add(result, toProtocolHex(packet.maxPacketSize, 3))

    add(result, packet.user)
    add(result, '\0')
    add(result, scramble323(packet.scrambleBuff[0..7], password))
    add(result, toProtocolHex(0, 1))
    add(result, packet.database)
    add(result, '\0')

when isMainModule:
  import asyncnet, asyncdispatch


  # echo repr toProtocolHex(0, 3)
  # echo toProtocolInt(toProtocolHex(76, 3))
  
  proc parse() {.async.} =
    var socket = newAsyncSocket(buffered = false)
    var p = initGreetingPacketParser()
    await connect(socket, "127.0.0.1", Port(3306))
    while true:
      var buf = await recv(socket, 3)
      parse(p, buf.cstring, buf.len)
      if p.finished:
        echo "offset: ", p.offset, " buf: ", buf.len
        echo p.packet
        break
    await send(socket, toPacketHex((
      capabilities: 521167,
      maxPacketSize: 0,
      charset: 33,
      user: "mysql",
      scrambleBuff: p.packet.scrambleBuff,
      database: "mysql"), p.sequenceId + 1, "123456", true))
    while true:
      var buf = await recv(socket, 1024)
      echo repr buf
      break

  proc main() {.async.} =
    try:
      await parse()
    except:
      echo getCurrentExceptionMsg() 

  waitFor main()  

  proc echoPacket() {.async.} =
    try:
      var socket = newAsyncSocket(buffered = false)
      await connect(socket, "127.0.0.1", Port(3306))
      var word: array[1024, char]
      let n = await recvInto(socket, word[0].addr, 1024)
      echo n
      echo cast[int](word[0])
      for i in 0..<n:
        write(stdout, toHex(word[i]), ' ')
      echo " "
      for i in 0..<n:
        write(stdout, (word[i]))
      echo " "
    except:
      echo getCurrentExceptionMsg() 
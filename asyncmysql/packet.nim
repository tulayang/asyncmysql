#    AsyncMysql - Asynchronous MySQL connector written in pure Nim
#        (c) Copyright 2017 Wang Tong
#
#    See the file "LICENSE", included in this distribution, for
#    details about the copyright.

#[ Handshake Initialization Packet
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
1              scramble payloadLen
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

const
  SERVER_STATUS_IN_TRANS* = 1
  SERVER_STATUS_AUTOCOMMIT* = 2
  SERVER_MORE_RESULTS_EXISTS* = 8
  SERVER_QUERY_NO_GOOD_INDEX_USED* = 16
  SERVER_QUERY_NO_INDEX_USED* = 32
  SERVER_STATUS_CURSOR_EXISTS* = 64
  SERVER_STATUS_LAST_ROW_SENT* = 128
  SERVER_STATUS_DB_DROPPED* = 256
  SERVER_STATUS_NO_BACKSLASH_ESCAPES* = 512
  SERVER_STATUS_METADATA_CHANGED* = 1024
  SERVER_QUERY_WAS_SLOW* = 2048
  SERVER_PS_OUT_PARAMS* = 4096
  SERVER_STATUS_IN_TRANS_READONLY* = 8192
  SERVER_SESSION_STATE_CHANGED* = 16384

proc toProtocolHex*(x: Natural, len: Positive): string =
  ## For example: `(0xFAFF, 2)` => `"\xFF\xFA"`, `(0xFAFF00, 3)` => `"\x00\xFF\xFA"`. 
  var n = x
  result = newString(len)
  for i in 0..<int(len):
    result[i] = chr(n and 0xFF)
    n = n shr 8

proc toProtocolInt*(str: string): Natural =
  ## For example: `"\xFF\xFA"` => `0xFAFF`, `"\x00\xFF\xFA"` => `0xFAFF00`.  
  result = 0
  var i = 0
  for c in str:
    inc(result, ord(c) shl (8 * i)) # c.int * pow(16.0, i.float32 * 2).int
    inc(i)

template offsetChar(x: pointer, i: int): pointer =
  cast[pointer](cast[ByteAddress](x) + i * sizeof(char))

template offsetCharVal(x: pointer, i: int): char =
  cast[ptr char](offsetChar(x, i))[]

proc joinFixedStr(s: var string, want: var int, buf: pointer, size: int) =
  # Joins `s` incrementally, `s` is a fixed length string. The first `want` is its length.
  # It is finished if want is `0` when returned, or not finished.
  let n = if size > want: want else: size
  for i in 0..<n:
    s.add(offsetCharVal(buf, i)) 
  dec(want, n)

proc joinNulStr(s: var string, buf: pointer, size: int): tuple[finished: bool, count: int] =
  # Joins `s` incrementally, `s` is a null terminated string. 
  result.finished = false
  for i in 0..<size:
    inc(result.count)
    if offsetCharVal(buf, i) == '\0':
      result.finished = true
      return
    else:
      s.add(offsetCharVal(buf, i))

type
  PacketState* = enum ## Parse state of the ``PacketParser``.
    packInitialization,           packPayloadLength,          packSequenceId,             
    packFinish, 
    packHandshakeProtocolVersion, packHandshakeServerVersion, packHandshakeThreadId, 
    packHandshakeScrambleBuff1,   packHandshakeFiller0,       packHandshakeCapabilities1, 
    packHandshakeCharSet,         packHandshakeStatus,        packHandshakeCapabilities2, 
    packHandshakeFiller1,         packHandshakeFiller2,       packHandshakeScrambleBuff2, 
    packHandshakeFiller3,         packHandshakePlugin, 
    packGenericHeader,
    packGenericOkAffectedRows,    packGenericOkLastInsertId,  packGenericOkServerStatus, 
    packGenericOkWarningCount,    packGenericOkStatusInfo,    packGenericOkSessionState,
    packGenericErrorCode,         packGenericErrorSqlState,   packGenericErrorSqlStateMarker,        
    packGenericErrorMessage,
    packGenericEofWarningCount,   packGenericEofServerStatus

    packResultSetColumnCount,     packResultSetCatalog,       packResultSetSchema, 
    packResultSetTable,           packResultSetOrgTable,      packResultSetName,
    packResultSetOrgName,         packResultSetFiller1,       packResultSetCharset,
    packResultSetColumnLen,       packResultSetColumnType,    packResultSetColumnFlags,
    packResultSetDecimals,        packResultSetFiller2,       packResultSetDefaultValue,
    packResultSetEof1,            packResultSetRowData,       packResultSetEof2       

  LenEncodedState* = enum ## Parse state for length encoded integer or string.
    encodedFlagVal, encodedIntVal, encodedStrVal

  ProgressState* = enum 
    progressOk, progressContinue, progressBreak

  NextState = enum
    nextLenEncoded

  PacketParser* = object ## The packet parser object.
    buf: pointer
    bufLen: int
    bufPos: int
    realLen: int
    packetPos: int
    word: string
    want: int
    payloadLen: int
    sequenceId: int
    wantPayloadLen: int
    storeWant: int
    isLast: bool
    state: PacketState
    storeState: PacketState
    encodedState: LenEncodedState

  HandshakePacket* = object       
    ## Packet from mysql server when connecting to the server that requires authentication.
    sequenceId*: int           # 1
    protocolVersion*: int      # 1
    serverVersion*: string     # NullTerminatedString
    threadId*: int             # 4
    scrambleBuff1*: string     # 8
    capabilities*: int         # (4)
    capabilities1*: int        # 2
    charset*: int              # 1
    serverStatus*: int         # 2
    capabilities2*: int        # [2]
    scrambleLen*: int          # [1]
    scrambleBuff2*: string     # [12]
    scrambleBuff*: string      # 8 + [12]
    plugin*: string            # NullTerminatedString 
    protocol41*: bool

  GenericPacketKind* = enum
    genericOk, genericError, genericEof  

  GenericPacket* = object
    sequenceId*: int           
    case kind*: GenericPacketKind
    of genericOk:
      affectedRows*: int
      lastInsertId*: int
      serverStatus*: int
      warningCount*: int
      message*: string
      sessionState*: string
    of genericError:
      errorCode*: int  
      sqlStateMarker*: string
      sqlState*: string
      errorMessage*: string
    of genericEof:
      warningCountOfEof*: int
      serverStatusOfEof*: int 

  EofPacket* = object
    warningCount*: int
    serverStatus*: int 

  ResultSetPacket* = object
    sequenceId*: int

    columnCount*: int
    extra*: string

    catalog*: string
    schema*: string
    table*: string
    orgTable*: string
    name*: string
    orgName*: string
    charset*: int
    columnLen*: int
    columnType*: int
    columnFlags*: int
    decimals*: int
    defaultValue*: string

    eofOfColumn*: EofPacket
    # ...


template cond(exp: untyped): untyped =
  case exp
  of progressOk:
    discard
  of progressContinue:
    continue
  of progressBreak:
    return

proc initHandshakePacket(packet: var HandshakePacket) =
  packet.serverVersion = ""
  packet.scrambleBuff1 = ""
  packet.scrambleBuff2 = ""
  packet.plugin = ""

proc initGenericPacket(packet: var GenericPacket, kind: GenericPacketKind) =
  packet.kind = kind
  case kind
  of genericOk:
    packet.message = ""
    packet.sessionState = ""
  of genericError:
    packet.sqlStateMarker = ""
    packet.sqlState = ""
    packet.errorMessage = ""
  of genericEof:
    discard

proc initResultSetPacket(packet: var ResultSetPacket) =
  packet.extra = ""
  packet.catalog = ""
  packet.schema = ""
  packet.table = ""
  packet.orgTable = ""
  packet.name = ""
  packet.orgName = ""
  packet.defaultValue = ""

proc initPacketParser*(): PacketParser = 
  ## TODO: opmitize buffer
  result.state = packInitialization
  result.encodedState = encodedFlagVal
  result.want = 3  

proc finished*(p: PacketParser): bool =
  result = p.state == packFinish

proc sequenceId*(parser: PacketParser): int = 
  result = parser.sequenceId

proc offset*(parser: PacketParser): int = 
  result = parser.bufPos

proc mount(p: var PacketParser, buf: pointer, size: int) = 
  p.buf = buf
  p.bufLen = size
  p.bufPos = 0
  if p.state != packInitialization and p.state != packPayloadLength and p.state != packSequenceId:
    p.realLen = if p.wantPayloadLen <= size: p.wantPayloadLen
                else: size

proc move(p: var PacketParser) = 
  p.storeState = p.state
  p.storeWant = p.want
  p.state = packPayloadLength
  p.want = 3  
  p.word = ""
  p.isLast = true
  p.realLen = 0
  
proc checkIfMove(p: var PacketParser): ProgressState =
  assert p.realLen == 0
  if p.bufLen > p.bufPos:
    assert p.wantPayloadLen == 0
    if p.isLast:
      raise newException(ValueError, "invalid packet")
    else:
      move(p)
      return progressContinue
  else: 
    if p.wantPayloadLen > 0:
      return progressBreak
    else: # == 0
      if p.isLast:
        raise newException(ValueError, "invalid packet")
      else:
        move(p)
        return progressContinue

proc next(p: var PacketParser, state: PacketState) =
  p.state = state

proc next(p: var PacketParser, state: PacketState, want: int) =
  p.state = state
  p.want = want

proc next(p: var PacketParser, state: PacketState, want: int, nextState: NextState) =
  p.state = state
  p.want = want
  case nextState
  of nextLenEncoded:
    p.encodedState = encodedFlagVal

proc parseFixed(p: var PacketParser, field: var int): ProgressState =
  result = progressOk
  let want = p.want
  joinFixedStr(p.word, p.want, offsetChar(p.buf, p.bufPos), p.realLen)
  let n = want - p.want
  inc(p.bufPos, n)
  dec(p.realLen, n)
  dec(p.wantPayloadLen, n)
  if p.want > 0:
    return checkIfMove(p)
  field = toProtocolInt(p.word)
  p.word = ""

proc parseFixed(p: var PacketParser, field: var string): ProgressState =
  result = progressOk
  let want = p.want
  joinFixedStr(field, p.want, offsetChar(p.buf, p.bufPos), p.realLen)
  let n = want - p.want
  inc(p.bufPos, n)
  dec(p.realLen, n)
  dec(p.wantPayloadLen, n)
  if p.want > 0:
    return checkIfMove(p)

proc parseNul(p: var PacketParser, field: var string): ProgressState =
  result = progressOk
  let (finished, count) = joinNulStr(field, offsetChar(p.buf, p.bufPos), p.realLen)
  inc(p.bufPos, count)
  dec(p.realLen, count)
  dec(p.wantPayloadLen, count)
  if not finished:
    return checkIfMove(p)

proc parseFiller(p: var PacketParser): ProgressState =
  result = progressOk
  if p.want > p.realLen:
    inc(p.bufPos, p.realLen)
    dec(p.wantPayloadLen, p.realLen)
    dec(p.want, p.realLen)
    dec(p.realLen, p.realLen)
    return checkIfMove(p)
  else:  
    let n = p.want
    inc(p.bufPos, n)
    dec(p.realLen, n)
    dec(p.wantPayloadLen, n)
    dec(p.want, n)

proc parseLenEncoded(p: var PacketParser, field: var int): ProgressState =
  while true:
    case p.encodedState
    of encodedFlagVal:
      var value: int
      let ret = parseFixed(p, value)
      if ret != progressOk:
        return ret
      assert value >= 0
      if value < 251:
        field = value
        return progressOk
      elif value == 0xFC:
        p.want = 2
      elif value == 0xFD:
        p.want = 3
      elif value == 0xFE:
        p.want = 8
      else:
        raise newException(ValueError, "invalid encoded flag")  
      p.encodedState = encodedIntVal
    of encodedIntVal:
      return parseFixed(p, field)
    else:
      raise newException(ValueError, "imposible state")

proc parseLenEncoded(p: var PacketParser, field: var string): ProgressState =
  while true:
    case p.encodedState
    of encodedFlagVal:
      var value: int
      let ret = parseFixed(p, value)
      if ret != progressOk:
        return ret
      assert value >= 0
      if value < 251:
        p.encodedState = encodedStrVal
        p.want = value
        continue
      elif value == 0xFC:
        p.want = 2
      elif value == 0xFD:
        p.want = 3
      elif value == 0xFE:
        p.want = 8
      else:
        raise newException(ValueError, "invalid encoded flag")  
      p.encodedState = encodedIntVal
    of encodedIntVal:
      var value: int
      let ret = parseFixed(p, value)
      if ret != progressOk:
        return ret
      p.want = value
      p.encodedState = encodedStrVal
    of encodedStrVal:
      return parseFixed(p, field)

proc parseOnPayloadLen(p: var PacketParser): ProgressState =
  result = progressOk
  let w = p.want
  joinFixedStr(p.word, p.want, offsetChar(p.buf, p.bufPos), p.bufLen - p.bufPos)
  inc(p.bufPos, w - p.want)
  if p.want > 0: 
    return progressBreak
  p.payloadLen = toProtocolInt(p.word)
  if p.payloadLen == 0xFFFFFF:
    p.isLast = false
  elif p.payloadLen == 0:
    p.isLast = true
  p.word = ""
  p.wantPayloadLen = p.payloadLen
  next(p, packSequenceId, 1)
  
proc parseOnSequenceId(p: var PacketParser): ProgressState =
  result = progressOk
  let w = p.want
  joinFixedStr(p.word, p.want, offsetChar(p.buf, p.bufPos), p.bufLen - p.bufPos)
  inc(p.bufPos, w - p.want)
  if p.want > 0:
    return progressBreak
  p.sequenceId = toProtocolInt(p.word)
  p.word = ""
  inc(p.packetPos)
  p.realLen = if p.bufLen - p.bufPos > p.wantPayloadLen: p.wantPayloadLen
              else: p.bufLen - p.bufPos
  next(p, p.storeState, p.storeWant)
  p.storeState = packInitialization

proc parse*(p: var PacketParser, packet: var HandshakePacket, buf: pointer, size: int) = 
  mount(p, buf, size)
  while true:
    case p.state
    of packInitialization:
      initHandshakePacket(packet)
      next(p, packHandshakeProtocolVersion, 1)
      move(p)
    of packPayloadLength:
      cond parseOnPayloadLen(p)
    of packSequenceId:
      cond parseOnSequenceId(p)
    of packHandshakeProtocolVersion:
      cond parseFixed(p, packet.protocolVersion)
      next(p, packHandshakeServerVersion)
    of packHandshakeServerVersion:
      cond parseNul(p, packet.serverVersion)
      next(p, packHandshakeThreadId, 4)
    of packHandshakeThreadId:
      cond parseFixed(p, packet.threadId)
      next(p, packHandshakeScrambleBuff1, 8)
    of packHandshakeScrambleBuff1:
      cond parseFixed(p, packet.scrambleBuff1)
      next(p, packHandshakeFiller0, 1)
    of packHandshakeFiller0:
      cond parseFiller(p)
      next(p, packHandshakeCapabilities1, 2)
    of packHandshakeCapabilities1:
      cond parseFixed(p, packet.capabilities1)
      next(p, packHandshakeCharSet, 1)
    of packHandshakeCharSet:
      cond parseFixed(p, packet.charset)
      next(p, packHandshakeStatus, 2)
    of packHandshakeStatus:
      cond parseFixed(p, packet.serverStatus)
      packet.protocol41 = (packet.capabilities1 and CLIENT_PROTOCOL_41) > 0
      if packet.protocol41:
        next(p, packHandshakeCapabilities2, 2)
      else:
        next(p, packHandshakeFiller3, 13)
    of packHandshakeCapabilities2:
      cond parseFixed(p, packet.capabilities2)
      packet.capabilities = packet.capabilities1 + 16 * packet.capabilities2
      next(p, packHandshakeFiller1, 1)
    of packHandshakeFiller1:
      cond parseFixed(p, packet.scrambleLen)
      next(p, packHandshakeFiller2, 10)
    of packHandshakeFiller2:
      cond parseFiller(p)
      # scrambleBuff2 should be 0x00 terminated, but sphinx does not do this
      # so we assume scrambleBuff2 to be 12 byte and treat the next byte as a
      # filler byte.
      next(p, packHandshakeScrambleBuff2, 12)
    of packHandshakeScrambleBuff2:
      cond parseFixed(p, packet.scrambleBuff2)
      packet.scrambleBuff = packet.scrambleBuff1 & packet.scrambleBuff2
      next(p, packHandshakeFiller3, 1)
    of packHandshakeFiller3:
      cond parseFiller(p)
      if p.isLast and p.wantPayloadLen == 0:
        next(p, packFinish)
      else:  
        next(p, packHandshakePlugin)
    of packHandshakePlugin:
      # According to the docs this should be 0x00 terminated, but MariaDB does
      # not do this, so we assume this string to be packet terminated.
      cond parseNul(p, packet.plugin)
      next(p, packFinish)
    of packFinish:
      packet.sequenceId = p.sequenceId
      return
    else:
      raise newException(ValueError, "imposible state")

proc parse*(parser: var PacketParser, packet: var HandshakePacket, buf: string) =
  ## Parse the ``buf`` data.
  parse(parser, packet, buf.cstring, buf.len)

proc parse*(p: var PacketParser, packet: var GenericPacket, handshakePacket: HandshakePacket, buf: pointer, size: int) = 
  mount(p, buf, size)
  while true:
    case p.state
    of packInitialization:
      next(p, packGenericHeader, 1)
      move(p)
    of packPayloadLength:
      cond parseOnPayloadLen(p)
    of packSequenceId:
      cond parseOnSequenceId(p)
    of packGenericHeader:
      var header: int
      cond parseFixed(p, header)
      case header
      of 0x00:
        initGenericPacket(packet, genericOk)
        next(p, packGenericOkAffectedRows, 1, nextLenEncoded)
      of 0xFE:
        initGenericPacket(packet, genericEof)
        if handshakePacket.protocol41:
          next(p, packGenericEofWarningCount, 2)
        else:
          next(p, packFinish)
      of 0xFF:
        initGenericPacket(packet, genericError)
        next(p, packGenericErrorCode, 2)
      else:
        raise newException(ValueError, "invalid header")
    of packGenericOkAffectedRows:
      cond parseLenEncoded(p, packet.affectedRows)
      next(p, packGenericOkLastInsertId, 1, nextLenEncoded)
    of packGenericOkLastInsertId:
      cond parseLenEncoded(p, packet.lastInsertId)
      if (handshakePacket.capabilities and CLIENT_PROTOCOL_41) > 0 or 
         (handshakePacket.capabilities and CLIENT_TRANSACTIONS) > 0:
        next(p, packGenericOkServerStatus, 2)
      elif (handshakePacket.capabilities and CLIENT_SESSION_TRACK) > 0:
        next(p, packGenericOkStatusInfo, 1, nextLenEncoded)
      else:
        next(p, packGenericOkStatusInfo, p.wantPayloadLen)
    of packGenericOkServerStatus:
      cond parseFixed(p, packet.serverStatus)
      if (handshakePacket.capabilities and CLIENT_PROTOCOL_41) > 0:
        next(p, packGenericOkWarningCount, 2)
      elif (handshakePacket.capabilities and CLIENT_SESSION_TRACK) > 0:
        next(p, packGenericOkStatusInfo, 1, nextLenEncoded)
      else:
        next(p, packGenericOkStatusInfo, p.wantPayloadLen)
    of packGenericOkWarningCount:
      cond parseFixed(p, packet.warningCount)
      if (handshakePacket.capabilities and CLIENT_SESSION_TRACK) > 0:
        next(p, packGenericOkStatusInfo, 1, nextLenEncoded)
      else:
        next(p, packGenericOkStatusInfo, p.wantPayloadLen)
    of packGenericOkStatusInfo:
      if (handshakePacket.capabilities and CLIENT_SESSION_TRACK) > 0:
        cond parseLenEncoded(p, packet.message)
        next(p, packGenericOkSessionState, 1, nextLenEncoded) 
      else:
        cond parseFixed(p, packet.message)
        next(p, packFinish)
    of packGenericOkSessionState:
      cond parseLenEncoded(p, packet.sessionState)
      next(p, packFinish)
    of packGenericErrorCode:
      cond parseFixed(p, packet.errorCode)
      if handshakePacket.protocol41:
        next(p, packGenericErrorSqlStateMarker, 1)
      else:
        next(p, packGenericErrorMessage, p.wantPayloadLen)
    of packGenericErrorSqlStateMarker:
      cond parseFixed(p, packet.sqlStateMarker)
      next(p, packGenericErrorSqlState, 5)
    of packGenericErrorSqlState:
      cond parseFixed(p, packet.sqlState)
      next(p, packGenericErrorMessage, p.wantPayloadLen)
    of packGenericErrorMessage:
      cond parseFixed(p, packet.errorMessage)
      next(p, packFinish)
    of packGenericEofWarningCount:
      cond parseFixed(p, packet.warningCountOfEof)
      next(p, packGenericEofServerStatus, 2)
    of packGenericEofServerStatus:
      cond parseFixed(p, packet.serverStatusOfEof)
      next(p, packFinish)
    of packFinish:
      packet.sequenceId = p.sequenceId
      return
    else:
      raise newException(ValueError, "imposible state")

proc parse*(parser: var PacketParser, packet: var GenericPacket, handshakePacket: HandshakePacket, buf: string) =
  ## Parse the ``buf`` data.
  parse(parser, packet, handshakePacket, buf.cstring, buf.len)

proc parse*(p: var PacketParser, packet: var ResultSetPacket, handshakePacket: HandshakePacket, buf: pointer, size: int) = 
  mount(p, buf, size)
  while true:
    case p.state
    of packInitialization:
      initResultSetPacket(packet)
      next(p, packResultSetColumnCount, 1)
      move(p)
    of packPayloadLength:
      cond parseOnPayloadLen(p)
    of packSequenceId:
      cond parseOnSequenceId(p)
      p.encodedState = encodedIntVal
    of packResultSetColumnCount:
      cond parseLenEncoded(p, packet.columnCount)
      next(p, packResultSetCatalog, 1, nextLenEncoded)
      move(p)
    of packResultSetCatalog:
      cond parseLenEncoded(p, packet.catalog)
      next(p, packResultSetSchema, 1, nextLenEncoded)
    of packResultSetSchema:
      cond parseLenEncoded(p, packet.schema)
      next(p, packResultSetTable, 1, nextLenEncoded)
    of packResultSetTable:
      cond parseLenEncoded(p, packet.table)
      next(p, packResultSetOrgTable, 1, nextLenEncoded)
    of packResultSetOrgTable:
      cond parseLenEncoded(p, packet.orgTable)
      next(p, packResultSetName, 1, nextLenEncoded)
    of packResultSetName:
      cond parseLenEncoded(p, packet.name)
      next(p, packResultSetOrgName, 1, nextLenEncoded)
    of packResultSetOrgName:
      cond parseLenEncoded(p, packet.orgName)
      next(p, packResultSetFiller1, 1, nextLenEncoded)
    of packResultSetFiller1:
      var fieldsLen: int
      cond parseLenEncoded(p, fieldsLen)
      assert fieldsLen == 0x0c
      next(p, packResultSetCharset, 2)
    of packResultSetCharset:
      cond parseFixed(p, packet.charset)
      next(p, packResultSetColumnLen, 4)
    of packResultSetColumnLen:
      cond parseFixed(p, packet.columnLen)
      next(p, packResultSetColumnType, 1)
    of packResultSetColumnType:
      cond parseFixed(p, packet.columnType)
      next(p, packResultSetColumnFlags, 2)
    of packResultSetColumnFlags:
      cond parseFixed(p, packet.columnFlags)
      next(p, packResultSetDecimals, 1)
    of packResultSetDecimals:
      cond parseFixed(p, packet.decimals)
      next(p, packResultSetFiller2, 2)
    of packResultSetFiller2:
      cond parseFiller(p)
      # if command == COM_FIELD_LIST:
      #   next(p, packResultSetDefaultValue, 1, nextLenEncoded)
      # else:
      next(p, packResultSetEof1)
    of packResultSetEof1:
      next(p, packFinish)
      # TODO 
    of packFinish:
      packet.sequenceId = p.sequenceId
      return
    else:
      raise newException(ValueError, "imposible state")

type
  ClientAuthenticationPacket* = object 
    ## Packet for login request.
    sequenceId*: int           # 1
    capabilities*: int         # 4
    maxPacketSize*: int        # 4
    charset*: int              # [1]
    # filler: string           # [23]
    user*: string              # NullTerminatedString
    # scrambleLen              # 1
    scrambleBuff*: string      # 20
    database*: string          # NullTerminatedString
    protocol41*: bool

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

proc format*(packet: ClientAuthenticationPacket, password: string): string = 
  var payloadLen: int
  if packet.protocol41:
    payloadLen = 4 + 4 + 1 + 23 + packet.user.len + 1 + 1 +
                    20 + packet.database.len + 1
    result = newStringOfCap(4 + payloadLen)
    add(result, toProtocolHex(payloadLen, 3))
    add(result, toProtocolHex(packet.sequenceId, 1))
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
    payloadLen = 2 + 3 + packet.user.len + 1 + 
                    8 + 1 + packet.database.len + 1
    result = newStringOfCap(4 + payloadLen)                
    add(result, toProtocolHex(payloadLen, 3))
    add(result, toProtocolHex(packet.sequenceId, 1))
    add(result, toProtocolHex(packet.capabilities, 2))
    add(result, toProtocolHex(packet.maxPacketSize, 3))
    add(result, packet.user)
    add(result, '\0')
    add(result, scramble323(packet.scrambleBuff[0..7], password))
    add(result, toProtocolHex(0, 1))
    add(result, packet.database)
    add(result, '\0')

type
  ServerCommand* = enum
    COM_SLEEP, 
    COM_QUIT, 
    COM_INIT_DB, 
    COM_QUERY, 
    COM_FIELD_LIST, 
    COM_CREATE_DB, 
    COM_DROP_DB, 
    COM_REFRESH, 
    COM_DEPRECATED_1, 
    COM_STATISTICS, 
    COM_PROCESS_INFO, 
    COM_CONNECT, 
    COM_PROCESS_KILL, 
    COM_DEBUG, 
    COM_PING, 
    COM_TIME, 
    COM_DELAYED_INSERT, 
    COM_CHANGE_USER, 
    COM_BINLOG_DUMP, 
    COM_TABLE_DUMP, 
    COM_CONNECT_OUT, 
    COM_REGISTER_SLAVE, 
    COM_STMT_PREPARE, 
    COM_STMT_EXECUTE, 
    COM_STMT_SEND_LONG_DATA, 
    COM_STMT_CLOSE, 
    COM_STMT_RESET, 
    COM_SET_OPTION, 
    COM_STMT_FETCH, 
    COM_DAEMON, 
    COM_BINLOG_DUMP_GTID, 
    COM_RESET_CONNECTION, COM_END

template formatNoArgsComImpl(cmd: ServerCommand) = 
  const payloadLen = 1
  result = newStringOfCap(4 + payloadLen)
  add(result, toProtocolHex(payloadLen, 3))
  add(result, toProtocolHex(0, 1))
  add(result, toProtocolHex(cmd.int, 1))

template formatRestStrComImpl(cmd: ServerCommand, str: string) = 
  let payloadLen = str.len + 1
  result = newStringOfCap(4 + payloadLen)
  add(result, toProtocolHex(payloadLen, 3))
  add(result, toProtocolHex(0, 1))
  add(result, toProtocolHex(cmd.int, 1))
  add(result, str)

proc formatComQuit*(): string = 
  formatNoArgsComImpl COM_QUIT

proc formatComInitDb*(dbname: string): string = 
  formatRestStrComImpl COM_INIT_DB, dbname

proc formatComQuery*(sql: string): string = 
  formatRestStrComImpl COM_QUERY, sql

proc formatComPing*(): string = 
  formatNoArgsComImpl COM_PING
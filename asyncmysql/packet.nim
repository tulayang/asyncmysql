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
  PacketParser* = object ## The packet parser object.
    buf: pointer
    bufLen: int
    bufPos: int
    bufRealLen: int
    packetPos: int
    word: string
    storeWord: string
    want: int
    payloadLen: int
    sequenceId: int
    wantPayloadLen: int
    storeWant: int
    isLast: bool
    state: PacketState
    storeState: PacketState
    wantEncodedState: LenEncodedState

  LenEncodedState* = enum ## Parse state for length encoded integer or string.
    lenFlagVal, lenIntVal, lenStrVal

  ProgressState* = enum 
    prgOk, prgContinue, prgBreak

  PacketState* = enum ## Parse state of the ``PacketParser``.
    packInitialization, 
    packHeader, 
    packFinish, 
    packHandshake, 
    packResultHeader,
    packResultOk, 
    packResultError,
    packResultSet

  HandshakeState* = enum
    hssProtocolVersion, hssServerVersion, hssThreadId, 
    hssScrambleBuff1,   hssFiller0,       hssCapabilities1, 
    hssCharSet,         hssStatus,        hssCapabilities2, 
    hssFiller1,         hssFiller2,       hssScrambleBuff2, 
    hssFiller3,         hssPlugin

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
    state: HandshakeState

  EofState* = enum
    eofHeader, eofWarningCount, eofServerStatus

  EofPacket* = object
    warningCount*: int
    serverStatus*: int 
    state: EofState

  ResultPacketKind* = enum
    rpkOk, rpkError, rpkResultSet  

  OkState* = enum
    okAffectedRows, okLastInsertId, okServerStatus, 
    okWarningCount, okStatusInfo,   okSessionState

  ErrorState* = enum
    errErrorCode, errSqlState, errSqlStateMarker, errErrorMessage

  ResultSetColumnState* = enum
    colCatalog,    colSchema,      colTable,       
    colOrgTable,   colName,        colOrgName,
    colFiller1,    colCharset,     colColumnLen,   
    colColumnType, colColumnFlags, colDecimals,    
    colFiller2,    colDefaultValue

  ResultSetColumnPacket* = object
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
    state: ResultSetColumnState

  ResultSetState* = enum
    rsetColumnHeader, rsetColumn, rsetColumnEof, rsetRowHeader, rsetRow, rsetRowEof

  ResultPacket* = object
    sequenceId*: int           
    case kind*: ResultPacketKind
    of rpkOk:
      affectedRows*: int
      lastInsertId*: int
      serverStatus*: int
      warningCount*: int
      message*: string
      sessionState*: string
      okState: OkState
    of rpkError:
      errorCode*: int  
      sqlStateMarker*: string
      sqlState*: string
      errorMessage*: string
      errState: ErrorState
    of rpkResultSet:
      extra*: string
      columnsCount*: int
      columnsPos: int
      columns*: seq[ResultSetColumnPacket]
      columnsEof*: EofPacket
      rowsCount*: int
      rowsPos: int
      rows*: seq[string]
      rowsEof*: EofPacket
      rsetState: ResultSetState

proc initHandshakePacket(packet: var HandshakePacket) =
  packet.serverVersion = ""
  packet.scrambleBuff1 = ""
  packet.scrambleBuff2 = ""
  packet.plugin = ""
  packet.state = hssProtocolVersion

proc initEofPacket(packet: var EofPacket) =
  packet.state = eofHeader   

proc initResultSetColumnPacket(packet: var ResultSetColumnPacket) =
  packet.catalog = ""
  packet.schema = ""
  packet.table = ""
  packet.orgTable = ""
  packet.name = ""
  packet.orgName = ""
  packet.defaultValue = ""
  packet.state = colCatalog

proc initResultPacket(packet: var ResultPacket, kind: ResultPacketKind) =
  packet.kind = kind
  case kind
  of rpkOk:
    packet.message = ""
    packet.sessionState = ""
    packet.okState = okAffectedRows
  of rpkError:
    packet.sqlStateMarker = ""
    packet.sqlState = ""
    packet.errorMessage = ""
    packet.errState = errErrorCode
  of rpkResultSet:
    packet.extra = ""
    packet.columns = @[]
    initEofPacket(packet.columnsEof)
    packet.rows = @[]
    initEofPacket(packet.rowsEof)
    packet.rsetState = rsetColumnHeader

proc initPacketParser*(): PacketParser = 
  ## TODO: opmitize buffer
  result.state = packInitialization
  result.wantEncodedState = lenFlagVal
  result.want = 4  
  result.word = ""

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
  if p.state != packInitialization and p.state != packHeader:
    p.bufRealLen = if p.wantPayloadLen <= size: p.wantPayloadLen
                   else: size

proc move(p: var PacketParser) = 
  assert p.bufRealLen == 0
  assert p.wantPayloadLen == 0
  p.storeState = p.state
  p.storeWord = p.word
  p.storeWant = p.want
  p.state = packHeader
  p.want = 4  
  p.word = ""

proc parseHeader(p: var PacketParser): ProgressState =
  result = prgOk
  let w = p.want
  joinFixedStr(p.word, p.want, offsetChar(p.buf, p.bufPos), p.bufLen - p.bufPos)
  inc(p.bufPos, w - p.want)
  if p.want > 0: 
    return prgBreak
  p.payloadLen = toProtocolInt(p.word[0..2])
  p.sequenceId = toProtocolInt(p.word[3..3])
  p.wantPayloadLen = p.payloadLen
  p.bufRealLen = if p.bufLen - p.bufPos > p.wantPayloadLen: p.wantPayloadLen
                 else: p.bufLen - p.bufPos
  inc(p.packetPos)
  if p.payloadLen == 0xFFFFFF:
    p.isLast = false
  elif p.payloadLen == 0:
    p.isLast = true
  p.word = p.storeWord
  p.want = p.storeWant
  p.state = p.storeState
  p.storeState = packInitialization

proc checkIfMove(p: var PacketParser): ProgressState =
  assert p.bufRealLen == 0
  if p.bufLen > p.bufPos:
    assert p.wantPayloadLen == 0
    if p.isLast:
      raise newException(ValueError, "invalid packet")
    else:
      move(p)
      return prgContinue
  else: 
    if p.wantPayloadLen > 0:
      return prgBreak
    else: # == 0
      if p.isLast:
        raise newException(ValueError, "invalid packet")
      else:
        move(p)
        return prgContinue

proc parseFixed(p: var PacketParser, field: var int): ProgressState =
  result = prgOk
  if p.want == 0:
    field = 0
    return
  if p.bufRealLen == 0:
    return checkIfMove(p)
  let want = p.want
  joinFixedStr(p.word, p.want, offsetChar(p.buf, p.bufPos), p.bufRealLen)
  let n = want - p.want
  inc(p.bufPos, n)
  dec(p.bufRealLen, n)
  dec(p.wantPayloadLen, n)
  if p.want > 0:
    return checkIfMove(p)
  field = toProtocolInt(p.word)
  p.word = ""

proc parseFixed(p: var PacketParser, field: var string): ProgressState =
  result = prgOk
  if p.want == 0:
    return
  if p.bufRealLen == 0:
    return checkIfMove(p)
  let want = p.want
  joinFixedStr(field, p.want, offsetChar(p.buf, p.bufPos), p.bufRealLen)
  let n = want - p.want
  inc(p.bufPos, n)
  dec(p.bufRealLen, n)
  dec(p.wantPayloadLen, n)
  if p.want > 0:
    return checkIfMove(p)

proc parseNul(p: var PacketParser, field: var string): ProgressState =
  result = prgOk
  if p.bufRealLen == 0:
    return checkIfMove(p)
  let (finished, count) = joinNulStr(field, offsetChar(p.buf, p.bufPos), p.bufRealLen)
  inc(p.bufPos, count)
  dec(p.bufRealLen, count)
  dec(p.wantPayloadLen, count)
  if not finished:
    return checkIfMove(p)

proc parseFiller(p: var PacketParser): ProgressState =
  result = prgOk
  if p.want > p.bufRealLen:
    inc(p.bufPos, p.bufRealLen)
    dec(p.wantPayloadLen, p.bufRealLen)
    dec(p.want, p.bufRealLen)
    dec(p.bufRealLen, p.bufRealLen)
    return checkIfMove(p)
  else:  
    let n = p.want
    inc(p.bufPos, n)
    dec(p.bufRealLen, n)
    dec(p.wantPayloadLen, n)
    dec(p.want, n)

proc parseLenEncoded(p: var PacketParser, field: var int): ProgressState =
  while true:
    case p.wantEncodedState
    of lenFlagVal:
      var value: int
      let ret = parseFixed(p, value)
      if ret != prgOk:
        return ret
      assert value >= 0
      if value < 251:
        field = value
        return prgOk
      elif value == 0xFC:
        p.want = 2
      elif value == 0xFD:
        p.want = 3
      elif value == 0xFE:
        p.want = 8
      else:
        raise newException(ValueError, "invalid encoded flag")  
      p.wantEncodedState = lenIntVal
    of lenIntVal:
      return parseFixed(p, field)
    else:
      raise newException(ValueError, "imposible state")

proc parseLenEncoded(p: var PacketParser, field: var string): ProgressState =
  while true:
    case p.wantEncodedState
    of lenFlagVal:
      var value: int
      let ret = parseFixed(p, value)
      if ret != prgOk:
        return ret
      assert value >= 0
      if value < 251:
        p.wantEncodedState = lenStrVal
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
      p.wantEncodedState = lenIntVal
    of lenIntVal:
      var value: int
      let ret = parseFixed(p, value)
      if ret != prgOk:
        return ret
      p.want = value
      p.wantEncodedState = lenStrVal
    of lenStrVal:
      return parseFixed(p, field)

template checkPrg(state: ProgressState): untyped =
  case state
  of prgOk:
    discard
  of prgContinue:
    continue
  of prgBreak:
    return

template checkIfOk(state: ProgressState): untyped =
  case state
  of prgOk:
    discard
  of prgContinue:
    return prgContinue
  of prgBreak:
    return prgBreak

proc parseHandshake(p: var PacketParser, packet: var HandshakePacket): ProgressState = 
  while true:
    case packet.state
    of hssProtocolVersion:
      checkIfOk parseFixed(p, packet.protocolVersion)
      packet.state = hssServerVersion
    of hssServerVersion:
      checkIfOk parseNul(p, packet.serverVersion)
      packet.state = hssThreadId
      p.want = 4
    of hssThreadId:
      checkIfOk parseFixed(p, packet.threadId)
      packet.state = hssScrambleBuff1
      p.want = 8
    of hssScrambleBuff1:
      checkIfOk parseFixed(p, packet.scrambleBuff1)
      packet.state = hssFiller0
      p.want = 1
    of hssFiller0:
      checkIfOk parseFiller(p)
      packet.state = hssCapabilities1
      p.want = 2
    of hssCapabilities1:
      checkIfOk parseFixed(p, packet.capabilities1)
      packet.state = hssCharSet
      p.want = 1
    of hssCharSet:
      checkIfOk parseFixed(p, packet.charset)
      packet.state = hssStatus
      p.want = 2
    of hssStatus:
      checkIfOk parseFixed(p, packet.serverStatus)
      packet.protocol41 = (packet.capabilities1 and CLIENT_PROTOCOL_41) > 0
      if packet.protocol41:
        packet.state = hssCapabilities2
        p.want = 2
      else:
        packet.state = hssFiller3
        p.want = 13
    of hssCapabilities2:
      checkIfOk parseFixed(p, packet.capabilities2)
      packet.capabilities = packet.capabilities1 + 65536 * packet.capabilities2 # 16*16*16*1
      packet.state = hssFiller1
      p.want = 1
    of hssFiller1:
      checkIfOk parseFixed(p, packet.scrambleLen)
      packet.state = hssFiller2
      p.want = 10
    of hssFiller2:
      checkIfOk parseFiller(p)
      # scrambleBuff2 should be 0x00 terminated, but sphinx does not do this
      # so we assume scrambleBuff2 to be 12 byte and treat the next byte as a
      # filler byte.
      packet.state = hssScrambleBuff2
      p.want = 12
    of hssScrambleBuff2:
      checkIfOk parseFixed(p, packet.scrambleBuff2)
      packet.scrambleBuff = packet.scrambleBuff1 & packet.scrambleBuff2
      packet.state = hssFiller3
      p.want = 1
    of hssFiller3:
      checkIfOk parseFiller(p)
      if p.isLast and p.wantPayloadLen == 0:
        return prgOk
      else:  
        packet.state = hssPlugin
    of hssPlugin:
      # According to the docs this should be 0x00 terminated, but MariaDB does
      # not do this, so we assume this string to be packet terminated.
      checkIfOk parseNul(p, packet.plugin)
      return prgOk

proc parse*(p: var PacketParser, packet: var HandshakePacket, buf: pointer, size: int) = 
  mount(p, buf, size)
  while true:
    case p.state
    of packInitialization:
      initHandshakePacket(packet)
      p.state = packHandshake
      p.want = 1
      move(p)
    of packHeader:
      checkPrg parseHeader(p)
    of packHandshake:
      checkPrg parseHandshake(p, packet)
      p.state = packFinish
    of packFinish:
      packet.sequenceId = p.sequenceId
      return
    else:
      raise newException(ValueError, "imposible state " & $p.state)

proc parse*(p: var PacketParser, packet: var HandshakePacket, buf: string) =
  ## Parse the ``buf`` data.
  parse(p, packet, buf.cstring, buf.len)

proc parseEof(p: var PacketParser, packet: var EofPacket, capabilities: int): ProgressState =
  while true:
    case packet.state
    of eofHeader:
      var header: int
      checkIfOk parseFixed(p, header)
      assert header == 0xFE
      if (capabilities and CLIENT_PROTOCOL_41) > 0:
        packet.state = eofWarningCount
        p.want = 2
      else:
        assert p.wantPayloadLen == 0
        return prgOk
    of eofWarningCount:
      checkIfOk parseFixed(p, packet.warningCount)
      packet.state = eofServerStatus
      p.want = 2
    of eofServerStatus:
      checkIfOk parseFixed(p, packet.serverStatus)
      assert p.wantPayloadLen == 0
      return prgOk

proc parseEof2(p: var PacketParser, packet: var EofPacket, capabilities: int): ProgressState =
  while true:
    case packet.state
    of eofHeader:
      if (capabilities and CLIENT_PROTOCOL_41) > 0:
        packet.state = eofWarningCount
        p.want = 2
      else:
        assert p.wantPayloadLen == 0
        return prgOk
    of eofWarningCount:
      checkIfOk parseFixed(p, packet.warningCount)
      packet.state = eofServerStatus
      p.want = 2
    of eofServerStatus:
      checkIfOk parseFixed(p, packet.serverStatus)
      assert p.wantPayloadLen == 0
      return prgOk

proc parseOk(p: var PacketParser, packet: var ResultPacket, capabilities: int): ProgressState =
  template nextStatusInfoWithTrack: untyped =
    packet.okState = okStatusInfo
    p.want = 1
    p.wantEncodedState = lenFlagVal
  template nextStatusInfoReset: untyped =
    packet.okState = okStatusInfo
    p.want = p.wantPayloadLen
  while true:
    case packet.okState
    of okAffectedRows:
      checkIfOk parseLenEncoded(p, packet.affectedRows)
      packet.okState = okLastInsertId
      p.want = 1
      p.wantEncodedState = lenFlagVal
    of okLastInsertId:
      echo "............", capabilities, " ", capabilities and CLIENT_PROTOCOL_41, " ", capabilities and CLIENT_TRANSACTIONS
      checkIfOk parseLenEncoded(p, packet.lastInsertId)
      if (capabilities and CLIENT_PROTOCOL_41) > 0 or 
         (capabilities and CLIENT_TRANSACTIONS) > 0:
        packet.okState = okServerStatus
        p.want = 2
      elif (capabilities and CLIENT_SESSION_TRACK) > 0:
        nextStatusInfoWithTrack
      else:
        nextStatusInfoReset
    of okServerStatus:
      echo "............", okServerStatus
      checkIfOk parseFixed(p, packet.serverStatus)
      if (capabilities and CLIENT_PROTOCOL_41) > 0:
        packet.okState = okWarningCount
        p.want = 2
      elif (capabilities and CLIENT_SESSION_TRACK) > 0:
        nextStatusInfoWithTrack
      else:
        nextStatusInfoReset
    of okWarningCount:
      echo "............", okWarningCount
      checkIfOk parseFixed(p, packet.warningCount)
      if (capabilities and CLIENT_SESSION_TRACK) > 0:
        nextStatusInfoWithTrack
      else:
        nextStatusInfoReset
    of okStatusInfo:
      echo "............", okStatusInfo
      if (capabilities and CLIENT_SESSION_TRACK) > 0:
        checkIfOk parseLenEncoded(p, packet.message)
        packet.okState = okSessionState
        p.want = 1
        p.wantEncodedState = lenFlagVal
      else:
        checkIfOk parseFixed(p, packet.message)
        return prgOk
    of okSessionState:
      echo "............", okSessionState
      checkIfOk parseLenEncoded(p, packet.sessionState)
      echo "............ okSessionState finish"
      return prgOk

proc parseError(p: var PacketParser, packet: var ResultPacket, capabilities: int): ProgressState =
  while true:
    case packet.errState
    of errErrorCode:
      checkIfOk parseFixed(p, packet.errorCode)
      if (capabilities and CLIENT_PROTOCOL_41) > 0:
        packet.errState = errSqlStateMarker
        p.want = 1
      else:
        packet.errState = errErrorMessage
        p.want = p.wantPayloadLen
    of errSqlStateMarker:
      checkIfOk parseFixed(p, packet.sqlStateMarker)
      packet.errState = errSqlState
      p.want = 5
    of errSqlState:
      checkIfOk parseFixed(p, packet.sqlState)
      packet.errState = errErrorMessage
      p.want = p.wantPayloadLen
    of errErrorMessage:
      checkIfOk parseFixed(p, packet.errorMessage)
      return prgOk

proc parseResultSetColumn(p: var PacketParser, packet: var ResultSetColumnPacket,
                          capabilities: int): ProgressState =
  while true:
    case packet.state
    of colCatalog:
      if (capabilities and CLIENT_PROTOCOL_41) > 0:
        checkIfOk parseLenEncoded(p, packet.catalog)
        packet.state = colSchema
        p.want = 1
        p.wantEncodedState = lenFlagVal
      else:
        packet.state = colTable
        p.want = 1
        p.wantEncodedState = lenFlagVal
    of colSchema:
      checkIfOk parseLenEncoded(p, packet.schema)
      packet.state = colTable
      p.want = 1
      p.wantEncodedState = lenFlagVal
    of colTable:
      checkIfOk parseLenEncoded(p, packet.table)
      if (capabilities and CLIENT_PROTOCOL_41) > 0:
        packet.state = colOrgTable
        p.want = 1
        p.wantEncodedState = lenFlagVal
      else:
        packet.state = colName
        p.want = 1
        p.wantEncodedState = lenFlagVal
    of colOrgTable:
      checkIfOk parseLenEncoded(p, packet.orgTable)
      packet.state = colName
      p.want = 1
      p.wantEncodedState = lenFlagVal
    of colName:
      checkIfOk parseLenEncoded(p, packet.name)
      if (capabilities and CLIENT_PROTOCOL_41) > 0:
        packet.state = colOrgName
        p.want = 1
        p.wantEncodedState = lenFlagVal
      else:
        packet.state = colColumnLen
        p.want = 4
    of colOrgName:
      checkIfOk parseLenEncoded(p, packet.orgName)
      packet.state = colFiller1
      p.want = 1
      p.wantEncodedState = lenFlagVal
    of colFiller1:
      var fieldsLen: int
      checkIfOk parseLenEncoded(p, fieldsLen)
      assert fieldsLen == 0x0c
      packet.state = colCharset
      p.want = 2
    of colCharset:
      checkIfOk parseFixed(p, packet.charset)
      packet.state = colColumnLen
      p.want = 4
    of colColumnLen:
      checkIfOk parseFixed(p, packet.columnLen)  
      packet.state = colColumnType
      if (capabilities and CLIENT_PROTOCOL_41) > 0:
        p.want = 1
      else:
        p.want = 2
    of colColumnType:
      checkIfOk parseFixed(p, packet.columnType)
      packet.state = colColumnFlags
      p.want = 2
    of colColumnFlags:
      checkIfOk parseFixed(p, packet.columnFlags)
      packet.state = colDecimals
      p.want = 1
    of colDecimals:
      checkIfOk parseFixed(p, packet.decimals)
      if (capabilities and CLIENT_PROTOCOL_41) > 0:
        packet.state = colFiller2
        p.want = 2
      else:
        packet.state = colDefaultValue
        p.want = p.wantPayloadLen
    of colFiller2:
      checkIfOk parseFiller(p)
      # if command == COM_FIELD_LIST:
      #   next(p, colDefaultValue, 1, nextLenEncoded)
      # else:
      # assert p.wantPayloadLen == 0
      # return prgOk
      packet.state = colDefaultValue
      p.want = p.wantPayloadLen
    of colDefaultValue:
      checkIfOk parseFixed(p, packet.defaultValue)
      assert p.wantPayloadLen == 0
      return prgOk

proc parseResultSet(p: var PacketParser, packet: var ResultPacket, capabilities: int): ProgressState =
  while true:
    case packet.rsetState
    of rsetColumnHeader:
      checkIfOk parseFixed(p, packet.extra)
      if packet.columnsCount > 0:
        for i in 0..<packet.columnsCount:
          var column: ResultSetColumnPacket
          initResultSetColumnPacket(column)
          packet.columns.add(column)
        packet.rsetState = rsetColumn
        p.want = 1
        p.wantEncodedState = lenFlagVal  
      else:
        packet.rsetState = rsetColumnEof
        p.want = 1
    of rsetColumn:
      checkIfOk parseResultSetColumn(p, packet.columns[packet.columnsPos], capabilities)
      inc(packet.columnsPos)
      if packet.columnsPos < packet.columnsCount:
        packet.rsetState = rsetColumn
        p.want = 1
        p.wantEncodedState = lenFlagVal  
      else:
        packet.rsetState = rsetColumnEof
        p.want = 1
    of rsetColumnEof:
      checkIfOk parseEof(p, packet.columnsEof, capabilities)
      packet.rsetState = rsetRowHeader
      p.want = 1
      p.wantEncodedState = lenFlagVal  
    of rsetRowHeader: 
      var header: int
      checkIfOk parseFixed(p, header)
      if header == 0xFE and p.payloadLen < 9:
        packet.rsetState = rsetRowEof
        p.want = 1
        continue
      packet.rowsCount = header
      packet.rsetState = rsetRow
      p.want = header
    of rsetRow:
      packet.rows.add("")
      checkIfOk parseFixed(p, packet.rows[packet.rowsPos])
      inc(packet.rowsPos)
      packet.rsetState = rsetRowHeader
      p.want = 1
      p.wantEncodedState = lenFlagVal 
    of rsetRowEof:
      checkIfOk parseEof2(p, packet.rowsEof, capabilities)
      return prgOk

proc parse*(p: var PacketParser, packet: var ResultPacket, capabilities: int, buf: pointer, size: int) = 
  mount(p, buf, size)
  while true:
    case p.state
    of packInitialization:
      p.state = packResultHeader
      p.want = 1
      move(p)
    of packHeader:
      checkPrg parseHeader(p)
    of packResultHeader:
      var header: int
      checkPrg parseFixed(p, header)
      case header
      of 0x00:
        initResultPacket(packet, rpkOk)
        p.state = packResultOk
        p.want = 1
        p.wantEncodedState = lenFlagVal
      of 0xFF:
        initResultPacket(packet, rpkError)
        p.state = packResultError
        p.want = 2
        p.wantEncodedState = lenFlagVal
      else:
        initResultPacket(packet, rpkResultSet)
        packet.columnsCount = header
        p.state = packResultSet
        p.want = p.wantPayloadLen
        # TODO extra
    of packResultOk:
      echo "..................", packResultOk
      checkPrg parseOk(p, packet, capabilities)
      echo "..................packResultOk finish"
      p.state = packFinish
    of packResultError:  
      checkPrg parseError(p, packet, capabilities)
      p.state = packFinish  
    of packResultSet:
      checkPrg parseResultSet(p, packet, capabilities)
      p.state = packFinish  
    of packFinish:
      echo "...................", packFinish
      packet.sequenceId = p.sequenceId
      return
    else:
      raise newException(ValueError, "imposible state " & $p.state)

proc parse*(p: var PacketParser, packet: var ResultPacket, capabilities: int, buf: string) =
  ## Parse the ``buf`` data.
  parse(p, packet, capabilities, buf.cstring, buf.len)

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
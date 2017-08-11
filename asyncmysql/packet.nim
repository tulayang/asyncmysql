#    AsyncMysql - Asynchronous MySQL connector written in pure Nim
#        (c) Copyright 2017 Wang Tong
#
#    See the file "LICENSE", included in this distribution, for
#    details about the copyright.

import strutils, securehash, math

type 
  # [WL#8754: Deprecate COM_XXX commands which are redundant.]
  # _<https://dev.mysql.com/worklog/task/?id=8754>
  ServerCommand* = enum ## Commands used by Client/Server Protocol to request mysql oparations. 
    COM_SLEEP, 
    COM_QUIT, 
    COM_INIT_DB, 
    COM_QUERY, 
    COM_FIELD_LIST,           # Deprecated, show fields sql statement
    COM_CREATE_DB,            # Deprecated, create table sql statement
    COM_DROP_DB,              # Deprecated, drop table sql statement
    COM_REFRESH,              # Deprecated, flush sql statement
    COM_DEPRECATED_1, 
    COM_STATISTICS, 
    COM_PROCESS_INFO,         # Deprecated, show processlist sql statement
    COM_CONNECT, 
    COM_PROCESS_KILL,         # Deprecated, kill connection/query sql statement
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
    COM_RESET_CONNECTION, 
    COM_END
  
const
  ## Character sets used by mysql.
  CHARSET_BIG5_CHINESE_CI*                = 1
  CHARSET_LATIN2_CZECH_CS*                = 2
  CHARSET_DEC8_SWEDISH_CI*                = 3
  CHARSET_CP850_GENERAL_CI*               = 4
  CHARSET_LATIN1_GERMAN1_CI*              = 5
  CHARSET_HP8_ENGLISH_CI*                 = 6
  CHARSET_KOI8R_GENERAL_CI*               = 7
  CHARSET_LATIN1_SWEDISH_CI*              = 8
  CHARSET_LATIN2_GENERAL_CI*              = 9
  CHARSET_SWE7_SWEDISH_CI*                = 10
  CHARSET_ASCII_GENERAL_CI*               = 11
  CHARSET_UJIS_JAPANESE_CI*               = 12
  CHARSET_SJIS_JAPANESE_CI*               = 13
  CHARSET_CP1251_BULGARIAN_CI*            = 14
  CHARSET_LATIN1_DANISH_CI*               = 15
  CHARSET_HEBREW_GENERAL_CI*              = 16
  CHARSET_TIS620_THAI_CI*                 = 18
  CHARSET_EUCKR_KOREAN_CI*                = 19
  CHARSET_LATIN7_ESTONIAN_CS*             = 20
  CHARSET_LATIN2_HUNGARIAN_CI*            = 21
  CHARSET_KOI8U_GENERAL_CI*               = 22
  CHARSET_CP1251_UKRAINIAN_CI*            = 23
  CHARSET_GB2312_CHINESE_CI*              = 24
  CHARSET_GREEK_GENERAL_CI*               = 25
  CHARSET_CP1250_GENERAL_CI*              = 26
  CHARSET_LATIN2_CROATIAN_CI*             = 27
  CHARSET_GBK_CHINESE_CI*                 = 28
  CHARSET_CP1257_LITHUANIAN_CI*           = 29
  CHARSET_LATIN5_TURKISH_CI*              = 30
  CHARSET_LATIN1_GERMAN2_CI*              = 31
  CHARSET_ARMSCII8_GENERAL_CI*            = 32
  CHARSET_UTF8_GENERAL_CI*                = 33
  CHARSET_CP1250_CZECH_CS*                = 34
  CHARSET_UCS2_GENERAL_CI*                = 35
  CHARSET_CP866_GENERAL_CI*               = 36
  CHARSET_KEYBCS2_GENERAL_CI*             = 37
  CHARSET_MACCE_GENERAL_CI*               = 38
  CHARSET_MACROMAN_GENERAL_CI*            = 39
  CHARSET_CP852_GENERAL_CI*               = 40
  CHARSET_LATIN7_GENERAL_CI*              = 41
  CHARSET_LATIN7_GENERAL_CS*              = 42
  CHARSET_MACCE_BIN*                      = 43
  CHARSET_CP1250_CROATIAN_CI*             = 44
  CHARSET_UTF8MB4_GENERAL_CI*             = 45
  CHARSET_UTF8MB4_BIN*                    = 46
  CHARSET_LATIN1_BIN*                     = 47
  CHARSET_LATIN1_GENERAL_CI*              = 48
  CHARSET_LATIN1_GENERAL_CS*              = 49
  CHARSET_CP1251_BIN*                     = 50
  CHARSET_CP1251_GENERAL_CI*              = 51
  CHARSET_CP1251_GENERAL_CS*              = 52
  CHARSET_MACROMAN_BIN*                   = 53
  CHARSET_UTF16_GENERAL_CI*               = 54
  CHARSET_UTF16_BIN*                      = 55
  CHARSET_UTF16LE_GENERAL_CI*             = 56
  CHARSET_CP1256_GENERAL_CI*              = 57
  CHARSET_CP1257_BIN*                     = 58
  CHARSET_CP1257_GENERAL_CI*              = 59
  CHARSET_UTF32_GENERAL_CI*               = 60
  CHARSET_UTF32_BIN*                      = 61
  CHARSET_UTF16LE_BIN*                    = 62
  CHARSET_BINARY*                         = 63
  CHARSET_ARMSCII8_BIN*                   = 64
  CHARSET_ASCII_BIN*                      = 65
  CHARSET_CP1250_BIN*                     = 66
  CHARSET_CP1256_BIN*                     = 67
  CHARSET_CP866_BIN*                      = 68
  CHARSET_DEC8_BIN*                       = 69
  CHARSET_GREEK_BIN*                      = 70
  CHARSET_HEBREW_BIN*                     = 71
  CHARSET_HP8_BIN*                        = 72
  CHARSET_KEYBCS2_BIN*                    = 73
  CHARSET_KOI8R_BIN*                      = 74
  CHARSET_KOI8U_BIN*                      = 75
  CHARSET_LATIN2_BIN*                     = 77
  CHARSET_LATIN5_BIN*                     = 78
  CHARSET_LATIN7_BIN*                     = 79
  CHARSET_CP850_BIN*                      = 80
  CHARSET_CP852_BIN*                      = 81
  CHARSET_SWE7_BIN*                       = 82
  CHARSET_UTF8_BIN*                       = 83
  CHARSET_BIG5_BIN*                       = 84
  CHARSET_EUCKR_BIN*                      = 85
  CHARSET_GB2312_BIN*                     = 86
  CHARSET_GBK_BIN*                        = 87
  CHARSET_SJIS_BIN*                       = 88
  CHARSET_TIS620_BIN*                     = 89
  CHARSET_UCS2_BIN*                       = 90
  CHARSET_UJIS_BIN*                       = 91
  CHARSET_GEOSTD8_GENERAL_CI*             = 92
  CHARSET_GEOSTD8_BIN*                    = 93
  CHARSET_LATIN1_SPANISH_CI*              = 94
  CHARSET_CP932_JAPANESE_CI*              = 95
  CHARSET_CP932_BIN*                      = 96
  CHARSET_EUCJPMS_JAPANESE_CI*            = 97
  CHARSET_EUCJPMS_BIN*                    = 98
  CHARSET_CP1250_POLISH_CI*               = 99
  CHARSET_UTF16_UNICODE_CI*               = 101
  CHARSET_UTF16_ICELANDIC_CI*             = 102
  CHARSET_UTF16_LATVIAN_CI*               = 103
  CHARSET_UTF16_ROMANIAN_CI*              = 104
  CHARSET_UTF16_SLOVENIAN_CI*             = 105
  CHARSET_UTF16_POLISH_CI*                = 106
  CHARSET_UTF16_ESTONIAN_CI*              = 107
  CHARSET_UTF16_SPANISH_CI*               = 108
  CHARSET_UTF16_SWEDISH_CI*               = 109
  CHARSET_UTF16_TURKISH_CI*               = 110
  CHARSET_UTF16_CZECH_CI*                 = 111
  CHARSET_UTF16_DANISH_CI*                = 112
  CHARSET_UTF16_LITHUANIAN_CI*            = 113
  CHARSET_UTF16_SLOVAK_CI*                = 114
  CHARSET_UTF16_SPANISH2_CI*              = 115
  CHARSET_UTF16_ROMAN_CI*                 = 116
  CHARSET_UTF16_PERSIAN_CI*               = 117
  CHARSET_UTF16_ESPERANTO_CI*             = 118
  CHARSET_UTF16_HUNGARIAN_CI*             = 119
  CHARSET_UTF16_SINHALA_CI*               = 120
  CHARSET_UTF16_GERMAN2_CI*               = 121
  CHARSET_UTF16_CROATIAN_MYSQL561_CI*     = 122
  CHARSET_UTF16_UNICODE_520_CI*           = 123
  CHARSET_UTF16_VIETNAMESE_CI*            = 124
  CHARSET_UCS2_UNICODE_CI*                = 128
  CHARSET_UCS2_ICELANDIC_CI*              = 129
  CHARSET_UCS2_LATVIAN_CI*                = 130
  CHARSET_UCS2_ROMANIAN_CI*               = 131
  CHARSET_UCS2_SLOVENIAN_CI*              = 132
  CHARSET_UCS2_POLISH_CI*                 = 133
  CHARSET_UCS2_ESTONIAN_CI*               = 134
  CHARSET_UCS2_SPANISH_CI*                = 135
  CHARSET_UCS2_SWEDISH_CI*                = 136
  CHARSET_UCS2_TURKISH_CI*                = 137
  CHARSET_UCS2_CZECH_CI*                  = 138
  CHARSET_UCS2_DANISH_CI*                 = 139
  CHARSET_UCS2_LITHUANIAN_CI*             = 140
  CHARSET_UCS2_SLOVAK_CI*                 = 141
  CHARSET_UCS2_SPANISH2_CI*               = 142
  CHARSET_UCS2_ROMAN_CI*                  = 143
  CHARSET_UCS2_PERSIAN_CI*                = 144
  CHARSET_UCS2_ESPERANTO_CI*              = 145
  CHARSET_UCS2_HUNGARIAN_CI*              = 146
  CHARSET_UCS2_SINHALA_CI*                = 147
  CHARSET_UCS2_GERMAN2_CI*                = 148
  CHARSET_UCS2_CROATIAN_MYSQL561_CI*      = 149
  CHARSET_UCS2_UNICODE_520_CI*            = 150
  CHARSET_UCS2_VIETNAMESE_CI*             = 151
  CHARSET_UCS2_GENERAL_MYSQL500_CI*       = 159
  CHARSET_UTF32_UNICODE_CI*               = 160
  CHARSET_UTF32_ICELANDIC_CI*             = 161
  CHARSET_UTF32_LATVIAN_CI*               = 162
  CHARSET_UTF32_ROMANIAN_CI*              = 163
  CHARSET_UTF32_SLOVENIAN_CI*             = 164
  CHARSET_UTF32_POLISH_CI*                = 165
  CHARSET_UTF32_ESTONIAN_CI*              = 166
  CHARSET_UTF32_SPANISH_CI*               = 167
  CHARSET_UTF32_SWEDISH_CI*               = 168
  CHARSET_UTF32_TURKISH_CI*               = 169
  CHARSET_UTF32_CZECH_CI*                 = 170
  CHARSET_UTF32_DANISH_CI*                = 171
  CHARSET_UTF32_LITHUANIAN_CI*            = 172
  CHARSET_UTF32_SLOVAK_CI*                = 173
  CHARSET_UTF32_SPANISH2_CI*              = 174
  CHARSET_UTF32_ROMAN_CI*                 = 175
  CHARSET_UTF32_PERSIAN_CI*               = 176
  CHARSET_UTF32_ESPERANTO_CI*             = 177
  CHARSET_UTF32_HUNGARIAN_CI*             = 178
  CHARSET_UTF32_SINHALA_CI*               = 179
  CHARSET_UTF32_GERMAN2_CI*               = 180
  CHARSET_UTF32_CROATIAN_MYSQL561_CI*     = 181
  CHARSET_UTF32_UNICODE_520_CI*           = 182
  CHARSET_UTF32_VIETNAMESE_CI*            = 183
  CHARSET_UTF8_UNICODE_CI*                = 192
  CHARSET_UTF8_ICELANDIC_CI*              = 193
  CHARSET_UTF8_LATVIAN_CI*                = 194
  CHARSET_UTF8_ROMANIAN_CI*               = 195
  CHARSET_UTF8_SLOVENIAN_CI*              = 196
  CHARSET_UTF8_POLISH_CI*                 = 197
  CHARSET_UTF8_ESTONIAN_CI*               = 198
  CHARSET_UTF8_SPANISH_CI*                = 199
  CHARSET_UTF8_SWEDISH_CI*                = 200
  CHARSET_UTF8_TURKISH_CI*                = 201
  CHARSET_UTF8_CZECH_CI*                  = 202
  CHARSET_UTF8_DANISH_CI*                 = 203
  CHARSET_UTF8_LITHUANIAN_CI*             = 204
  CHARSET_UTF8_SLOVAK_CI*                 = 205
  CHARSET_UTF8_SPANISH2_CI*               = 206
  CHARSET_UTF8_ROMAN_CI*                  = 207
  CHARSET_UTF8_PERSIAN_CI*                = 208
  CHARSET_UTF8_ESPERANTO_CI*              = 209
  CHARSET_UTF8_HUNGARIAN_CI*              = 210
  CHARSET_UTF8_SINHALA_CI*                = 211
  CHARSET_UTF8_GERMAN2_CI*                = 212
  CHARSET_UTF8_CROATIAN_MYSQL561_CI*      = 213
  CHARSET_UTF8_UNICODE_520_CI*            = 214
  CHARSET_UTF8_VIETNAMESE_CI*             = 215
  CHARSET_UTF8_GENERAL_MYSQL500_CI*       = 223
  CHARSET_UTF8MB4_UNICODE_CI*             = 224
  CHARSET_UTF8MB4_ICELANDIC_CI*           = 225
  CHARSET_UTF8MB4_LATVIAN_CI*             = 226
  CHARSET_UTF8MB4_ROMANIAN_CI*            = 227
  CHARSET_UTF8MB4_SLOVENIAN_CI*           = 228
  CHARSET_UTF8MB4_POLISH_CI*              = 229
  CHARSET_UTF8MB4_ESTONIAN_CI*            = 230
  CHARSET_UTF8MB4_SPANISH_CI*             = 231
  CHARSET_UTF8MB4_SWEDISH_CI*             = 232
  CHARSET_UTF8MB4_TURKISH_CI*             = 233
  CHARSET_UTF8MB4_CZECH_CI*               = 234
  CHARSET_UTF8MB4_DANISH_CI*              = 235
  CHARSET_UTF8MB4_LITHUANIAN_CI*          = 236
  CHARSET_UTF8MB4_SLOVAK_CI*              = 237
  CHARSET_UTF8MB4_SPANISH2_CI*            = 238
  CHARSET_UTF8MB4_ROMAN_CI*               = 239
  CHARSET_UTF8MB4_PERSIAN_CI*             = 240
  CHARSET_UTF8MB4_ESPERANTO_CI*           = 241
  CHARSET_UTF8MB4_HUNGARIAN_CI*           = 242
  CHARSET_UTF8MB4_SINHALA_CI*             = 243
  CHARSET_UTF8MB4_GERMAN2_CI*             = 244
  CHARSET_UTF8MB4_CROATIAN_MYSQL561_CI*   = 245
  CHARSET_UTF8MB4_UNICODE_520_CI*         = 246
  CHARSET_UTF8MB4_VIETNAMESE_CI*          = 247
  CHARSET_UTF8_GENERAL50_CI*              = 253

  ## Values for the capabilities flag bitmask used by Client/Server Protocol.
  ## Currently need to fit into 32 bits.
  ## Each bit represents an optional feature of the protocol.
  ## Both the client and the server are sending these.
  ## The intersection of the two determines whast optional parts of the protocol will be used.
  CLIENT_LONG_PASSWORD*                   = 1 shl 0
    ## Use the improved version of Old Password Authentication.
  CLIENT_FOUND_ROWS*                      = 1 shl 1 
    ## Send found rows instead of affected rows in EOF_Packet.
  CLIENT_LONG_FLAG*                       = 1 shl 2 
    ## Get all field flags.
  CLIENT_CONNECT_WITH_DB*                 = 1 shl 3 
    ## Database (schema) name can be specified on connect in Handshake Response Packet.
  CLIENT_NO_SCHEMA*                       = 1 shl 4 
    ## Don't allow database.table.field.
  CLIENT_COMPRESS*                        = 1 shl 5
    ## Compression protocol supported. 
  CLIENT_ODBC*                            = 1 shl 6 
    ## Special handling of ODBC behavior. 
  CLIENT_LOCAL_FILES*                     = 1 shl 7 
    ## Can use LOAD DATA LOCAL.
  CLIENT_IGNORE_SPACE*                    = 1 shl 8
    ## Ignore spaces before '('.
  CLIENT_PROTOCOL_41*                     = 1 shl 9 
    ## New 4.1 protocol.
  CLIENT_INTERACTIVE*                     = 1 shl 10 
    ## This is an interactive client.
  CLIENT_SSL*                             = 1 shl 11
    ## Use SSL encryption for the session.
  CLIENT_IGNORE_SIGPIPE*                  = 1 shl 12
    ## Client only flag. 
  CLIENT_TRANSACTIONS*                    = 1 shl 13 
    ## Client knows about transactions.
  CLIENT_RESERVED*                        = 1 shl 14
    ## DEPRECATED: Old flag for 4.1 protocol.
  CLIENT_RESERVED2*                       = 1 shl 15 
    ## DEPRECATED: Old flag for 4.1 authentication.
  CLIENT_MULTI_STATEMENTS*                = 1 shl 16 
    ## Enable/disable multi-stmt support.
  CLIENT_MULTI_RESULTS*                   = 1 shl 17 
    ## Enable/disable multi-results.
  CLIENT_PS_MULTI_RESULTS*                = 1 shl 18    
    ## Multi-results and OUT parameters in PS-protocol. 
  CLIENT_PLUGIN_AUTH *                    = 1 shl 19
    ## Client supports plugin authentication.
  CLIENT_CONNECT_ATTRS*                   = 1 shl 20 
    ## Client supports connection attributes.
  CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA*  = 1 shl 21
    ## Enable authentication response packet to be larger than 255 bytes.
  CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS*    = 1 shl 22 
    ## Don't close the connection for a user account with expired password.
  CLIENT_SESSION_TRACK*                   = 1 shl 23
    ## Capable of handling server state change information. 
  CLIENT_DEPRECATE_EOF*                   = 1 shl 24 
    ## Client no longer needs EOF_Packet and will use OK_Packet instead.
  CLIENT_SSL_VERIFY_SERVER_CERT*          = 1 shl 30
    ## Verify server certificate.
  CLIENT_REMEMBER_OPTIONS*                = 1 shl 31
    ## Don't reset the options after an unsuccessful connect.

  ## Server status used by Client/Server Protocol.
  SERVER_STATUS_IN_TRANS*                 = 1
    ## Is raised when a multi-statement transaction has been started, either 
    ## explicitly, by means of BEGIN or COMMIT AND CHAIN, or implicitly, by 
    ## the first transactional statement, when autocommit=off.
  SERVER_STATUS_AUTOCOMMIT*               = 2
    ## Server in auto_commit mode.
  SERVER_MORE_RESULTS_EXISTS*             = 8
    ## Multi query - next query exists.
  SERVER_QUERY_NO_GOOD_INDEX_USED*        = 16
  SERVER_QUERY_NO_INDEX_USED*             = 32
  SERVER_STATUS_CURSOR_EXISTS*            = 64
    ## The server was able to fulfill the clients request and opened a read-only 
    ## non-scrollable cursor for a query.
  SERVER_STATUS_LAST_ROW_SENT*            = 128
    ## This flag is sent when a read-only cursor is exhausted, in reply to COM_STMT_FETCH command.
  SERVER_STATUS_DB_DROPPED*               = 256
    ## A database was dropped.
  SERVER_STATUS_NO_BACKSLASH_ESCAPES*     = 512
  SERVER_STATUS_METADATA_CHANGED*         = 1024
    ## Sent to the client if after a prepared statement reprepare we discovered 
    ## that the new statement returns a different number of result set fields.
  SERVER_QUERY_WAS_SLOW*                  = 2048
  SERVER_PS_OUT_PARAMS*                   = 4096
    ## To mark ResultSet containing output parameter values.
  SERVER_STATUS_IN_TRANS_READONLY*        = 8192
    ## Set at the same time as SERVER_STATUS_IN_TRANS if the started multi-statement transaction 
    ## is a read-only transaction.
  SERVER_SESSION_STATE_CHANGED*           = 16384
    ## This status flag, when on, implies that one of the state information has changed on 
    ## the server because of the execution of the last statement.

  ## Field(cloumn) types. 
  # Manually extracted from mysql-5.7.9/include/mysql.h.pp
  # some more info here: http://dev.mysql.com/doc/refman/5.5/en/c-api-prepared-statement-type-codes.html
  FIELD_TYPE_DECIMAL     = 0x00 ## aka DECIMAL (http://dev.mysql.com/doc/refman/5.0/en/precision-math-decimal-changes.html)
  FIELD_TYPE_TINY        = 0x01 ## aka TINYINT, 1 byte
  FIELD_TYPE_SHORT       = 0x02 ## aka SMALLINT, 2 bytes
  FIELD_TYPE_LONG        = 0x03 ## aka INT, 4 bytes
  FIELD_TYPE_FLOAT       = 0x04 ## aka FLOAT, 4-8 bytes
  FIELD_TYPE_DOUBLE      = 0x05 ## aka DOUBLE, 8 bytes
  FIELD_TYPE_NULL        = 0x06 ## NULL (used for prepared statements, I think)
  FIELD_TYPE_TIMESTAMP   = 0x07 ## aka TIMESTAMP
  FIELD_TYPE_LONGLONG    = 0x08 ## aka BIGINT, 8 bytes
  FIELD_TYPE_INT24       = 0x09 ## aka MEDIUMINT, 3 bytes
  FIELD_TYPE_DATE        = 0x0a ## aka DATE
  FIELD_TYPE_TIME        = 0x0b ## aka TIME
  FIELD_TYPE_DATETIME    = 0x0c ## aka DATETIME
  FIELD_TYPE_YEAR        = 0x0d ## aka YEAR, 1 byte (don't ask)
  FIELD_TYPE_NEWDATE     = 0x0e ## aka ?
  FIELD_TYPE_VARCHAR     = 0x0f ## aka VARCHAR (?)
  FIELD_TYPE_BIT         = 0x10 ## aka BIT, 1-8 byte
  FIELD_TYPE_TIMESTAMP2  = 0x11 ## aka TIMESTAMP with fractional seconds
  FIELD_TYPE_DATETIME2   = 0x12 ## aka DATETIME with fractional seconds
  FIELD_TYPE_TIME2       = 0x13 ## aka TIME with fractional seconds
  FIELD_TYPE_JSON        = 0xf5 ## aka JSON
  FIELD_TYPE_NEWDECIMAL  = 0xf6 ## aka DECIMAL
  FIELD_TYPE_ENUM        = 0xf7 ## aka ENUM
  FIELD_TYPE_SET         = 0xf8 ## aka SET
  FIELD_TYPE_TINY_BLOB   = 0xf9 ## aka TINYBLOB, TINYTEXT
  FIELD_TYPE_MEDIUM_BLOB = 0xfa ## aka MEDIUMBLOB, MEDIUMTEXT
  FIELD_TYPE_LONG_BLOB   = 0xfb ## aka LONGBLOG, LONGTEXT
  FIELD_TYPE_BLOB        = 0xfc ## aka BLOB, TEXT
  FIELD_TYPE_VAR_STRING  = 0xfd ## aka VARCHAR, VARBINARY
  FIELD_TYPE_STRING      = 0xfe ## aka CHAR, BINARY
  FIELD_TYPE_GEOMETRY    = 0xff ## aka GEOMETRY

  ## Field(cloumn) flags.
  # Manually extracted from mysql-5.5.23/include/mysql_com.h
  FIELD_FLAG_NOT_NULL_FLAG         = 1      ## Field can't be NULL 
  FIELD_FLAG_PRI_KEY_FLAG          = 2      ## Field is part of a primary key 
  FIELD_FLAG_UNIQUE_KEY_FLAG       = 4      ## Field is part of a unique key 
  FIELD_FLAG_MULTIPLE_KEY_FLAG     = 8      ## Field is part of a key 
  FIELD_FLAG_BLOB_FLAG             = 16     ## Field is a blob 
  FIELD_FLAG_UNSIGNED_FLAG         = 32     ## Field is unsigned 
  FIELD_FLAG_ZEROFILL_FLAG         = 64     ## Field is zerofill 
  FIELD_FLAG_BINARY_FLAG           = 128    ## Field is binary   
  # The following are only sent to new clients 
  FIELD_FLAG_ENUM_FLAG             = 256    ## Field is an enum 
  FIELD_FLAG_AUTO_INCREMENT_FLAG   = 512    ## Field is a autoincrement field 
  FIELD_FLAG_TIMESTAMP_FLAG        = 1024   ## Field is a timestamp 
  FIELD_FLAG_SET_FLAG              = 2048   ## Field is a set 
  FIELD_FLAG_NO_DEFAULT_VALUE_FLAG = 4096   ## Field doesn't have default value 
  FIELD_FLAG_ON_UPDATE_NOW_FLAG    = 8192   ## Field is set to NOW on UPDATE 
  FIELD_FLAG_NUM_FLAG              = 32768  ## Field is num (for clients) 

proc toProtocolHex*(x: Natural, len: Positive): string =
  ## Converts ``x`` to a string in the format of mysql Client/Server Protocol.
  ## For example: `(0xFAFF, 2)` => `"\xFF\xFA"`, `(0xFAFF00, 3)` => `"\x00\xFF\xFA"`. 
  var n = x
  result = newString(len)
  for i in 0..<int(len):
    result[i] = chr(n and 0xFF)
    n = n shr 8

proc toProtocolInt*(str: string): Natural =
  ## Converts ``str`` to a nonnegative integer.
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

proc joinFixedStr(s: pointer, sLen: int, want: var int, buf: pointer, size: int) =
  # Joins `s` incrementally.
  # It is finished if want is `0` when returned, or not finished.
  var n: int
  if sLen < want:
    if size < sLen:
      n = size
    else:
      n = sLen
  else: 
    if size < want:
      n = size
    else:
      n = want  
  copyMem(s, buf, n)
  dec(want, n)

proc joinFixedStr(s: var string, want: var int, buf: pointer, size: int) =
  # Joins `s` incrementally, `s` is a fixed length string. The first `want` is its length.
  # It is finished if want is `0` when returned, or not finished.
  let n = if size > want: want else: size
  for i in 0..<n:
    add(s, offsetCharVal(buf, i)) 
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
      add(s, offsetCharVal(buf, i))

type
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
  PacketParserKind* = enum ## Kinds of ``PacketParser``.
    ppkHandshake, ppkCommandResult 

  PacketParser* = object ## Parser that is used to parse a Mysql Client/Server Protocol packet.
    buf: pointer
    bufLen: int
    bufPos: int
    bufRealLen: int
    word: string
    want: int
    payloadLen: int
    sequenceId: int
    remainingPayloadLen: int
    storedWord: string
    storedWant: int
    storedState: PacketState
    state: PacketState
    wantEncodedState: LenEncodedState
    case kind: PacketParserKind
    of ppkHandshake:
      discard
    of ppkCommandResult:
      command: ServerCommand 
    isEntire: bool

  LenEncodedState = enum # Parse state for length encoded integer or string.
    lenFlagVal, lenIntVal, lenStrVal

  ProgressState = enum # Progress state for parsing.
    prgOk, prgNext, prgEmpty

  PacketState = enum # Parsing state of the ``PacketParser``.
    packInit, 
    packHeader, 
    packFinish, 
    packHandshake, 
    packResultHeader,
    packResultOk, 
    packResultError,
    packResultSetFields, 
    packResultSetRows

  HandshakeState = enum # Parse state for handshaking.
    hssProtocolVersion, 
    hssServerVersion, 
    hssThreadId, 
    hssScrambleBuff1,   
    hssFiller0,       
    hssCapabilities1, 
    hssCharSet,         
    hssStatus,        
    hssCapabilities2, 
    hssFiller1,         
    hssFiller2,       
    hssScrambleBuff2, 
    hssFiller3,         
    hssPlugin

  HandshakePacket* = object       
    ## Packet from mysql server when connecting to the server that requires authentication.
    sequenceId*: int           # 1
    protocolVersion*: int      # 1
    serverVersion*: string     # NullTerminatedString
    threadId*: int             # 4
    scrambleBuff1: string      # 8
    capabilities*: int         # (4)
    capabilities1: int         # 2
    charset*: int              # 1
    serverStatus*: int         # 2
    capabilities2: int         # [2]
    scrambleLen*: int          # [1]
    scrambleBuff2: string      # [12]
    scrambleBuff*: string      # 8 + [12]
    plugin*: string            # NullTerminatedString 
    protocol41*: bool
    state: HandshakeState

  EofState = enum
    eofHeader, 
    eofWarningCount, 
    eofServerStatus

  EofPacket* = object
    warningCount*: int
    serverStatus*: int 
    state: EofState

  ResultPacketKind* = enum ## Kinds of result packet.
    rpkOk, rpkError, rpkResultSet  

  OkState = enum
    okAffectedRows, 
    okLastInsertId, 
    okServerStatus, 
    okWarningCount, 
    okStatusInfo,   
    okSessionState

  ErrorState = enum
    errErrorCode, 
    errSqlState, 
    errSqlStateMarker, 
    errErrorMessage

  FieldState = enum
    fieldCatalog,    
    fieldSchema,      
    fieldTable,       
    fieldOrgTable,   
    fieldName,        
    fieldOrgName,
    fieldFiller1,    
    fieldCharset,     
    fieldLen,   
    fieldType,       
    fieldFlags,       
    fieldDecimals,    
    fieldFiller2,    
    fieldDefaultValue

  FieldPacket* = object 
    catalog*: string
    schema*: string
    table*: string
    orgTable*: string
    name*: string
    orgName*: string
    charset*: int
    fieldLen*: int
    fieldType*: int
    fieldFlags*: int
    decimals*: int
    defaultValue*: string
    state: FieldState

  ResultSetState = enum
    rsetExtra, 
    rsetFieldHeader, 
    rsetField, 
    rsetFieldEof, 
    rsetRowHeader, 
    rsetRow, 
    rsetRowEof

  RowsState* = enum
    rowsFieldBegin,
    rowsFieldFull,
    rowsFieldEnd,
    rowsBufEmpty,
    rowsFinished

  ResultPacket* = object ## The result packet object.
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
      fieldsPos: int
      fields*: seq[FieldPacket]
      fieldsEof: EofPacket
      rowsEof: EofPacket
      rsetState: ResultSetState
      hasRows*: bool
      meetNull: bool
    hasMoreResults*: bool

  RowList* = object
    value*: seq[string]
    counter: int

proc initHandshakePacket(): HandshakePacket =
  result.sequenceId = 0
  result.protocolVersion = 0
  result.serverVersion = ""
  result.threadId = 0
  result.scrambleBuff1 = ""
  result.capabilities = 0
  result.capabilities1 = 0
  result.charset = 0
  result.serverStatus = 0
  result.capabilities2 = 0
  result.scrambleLen = 0
  result.scrambleBuff2 = ""
  result.scrambleBuff = ""
  result.plugin = ""
  result.protocol41 = true
  result.state = hssProtocolVersion

proc initEofPacket(): EofPacket =
  result.warningCount = 0
  result.serverStatus = 0
  result.state = eofHeader   

proc initFieldPacket(): FieldPacket =
  result.catalog = ""
  result.schema = ""
  result.table = ""
  result.orgTable = ""
  result.name = ""
  result.orgName = ""
  result.charset = 0
  result.fieldLen = 0
  result.fieldType = 0
  result.fieldFlags = 0
  result.decimals = 0
  result.defaultValue = ""
  result.state = fieldCatalog

proc initResultPacket(kind: ResultPacketKind): ResultPacket =
  result.kind = kind
  case kind
  of rpkOk:
    result.affectedRows = 0
    result.lastInsertId = 0
    result.serverStatus = 0
    result.warningCount = 0
    result.message = ""
    result.sessionState = ""
    result.okState = okAffectedRows
  of rpkError:
    result.errorCode = 0
    result.sqlStateMarker = ""
    result.sqlState = ""
    result.errorMessage = ""
    result.errState = errErrorCode
  of rpkResultSet:
    result.extra = ""
    result.fieldsPos = 0
    result.fields = @[]
    result.fieldsEof = initEofPacket()
    result.rowsEof = initEofPacket()
    result.rsetState = rsetExtra
    result.hasRows = false
    result.meetNull = false
  result.hasMoreResults = false

proc initRowList*(): RowList =
  result.value = @[]
  result.counter = -1

template initPacketParserImpl() = 
  result.buf = nil
  result.bufLen = 0
  result.bufPos = 0
  result.bufRealLen = 0
  result.word = ""
  result.want = 4  
  result.payloadLen = 0
  result.sequenceId = 0
  result.remainingPayloadLen = 0
  result.storedWord = nil
  result.storedWant = 0
  result.storedState = packInit
  result.state = packInit
  result.wantEncodedState = lenFlagVal
  result.isEntire = true 

proc initPacketParser*(kind: PacketParserKind): PacketParser = 
  ## Creates a new packet parser for parsing a handshake connection.
  initPacketParserImpl
  result.kind = kind

proc initPacketParser*(command: ServerCommand): PacketParser = 
  ## Creates a new packet parser for receiving a result packet.
  initPacketParserImpl
  result.kind = ppkCommandResult
  result.command = command
  
proc finished*(p: PacketParser): bool =
  ## Determines whether ``p`` has completed.
  result = p.state == packFinish

proc sequenceId*(parser: PacketParser): int = 
  ## Gets the current sequence ID.
  result = parser.sequenceId

proc offset*(parser: PacketParser): int =
  ## Gets the offset of the latest buffer. 
  result = parser.bufPos

proc buffered*(p: PacketParser): bool =
  result = p.bufPos < p.bufLen

proc mount*(p: var PacketParser, buf: pointer, size: int) = 
  p.buf = buf
  p.bufLen = size
  p.bufPos = 0
  if p.state != packInit and p.state != packHeader:
    p.bufRealLen = if p.remainingPayloadLen <= size: p.remainingPayloadLen
                   else: size

proc mount*(p: var PacketParser, buf: string) =
  mount(p, buf.cstring, buf.len)

proc move(p: var PacketParser) = 
  assert p.bufRealLen == 0
  assert p.remainingPayloadLen == 0
  p.storedState = p.state
  p.storedWant = p.want
  p.storedWord = p.word
  p.state = packHeader
  p.want = 4  
  p.word = ""
  p.isEntire = true 

proc parseHeader(p: var PacketParser): ProgressState =
  result = prgOk
  let w = p.want
  joinFixedStr(p.word, p.want, offsetChar(p.buf, p.bufPos), p.bufLen - p.bufPos)
  inc(p.bufPos, w - p.want)
  if p.want > 0: 
    return prgEmpty
  p.payloadLen = toProtocolInt(p.word[0..2])
  p.sequenceId = toProtocolInt(p.word[3..3])
  p.remainingPayloadLen = p.payloadLen
  p.bufRealLen = if p.bufLen - p.bufPos > p.remainingPayloadLen: p.remainingPayloadLen
                 else: p.bufLen - p.bufPos
  if p.payloadLen == 0xFFFFFF:
    p.isEntire = false
  elif p.payloadLen == 0:
    p.isEntire = true
  p.state = p.storedState
  p.want = p.storedWant
  p.word = p.storedWord
  p.storedState = packInit

proc checkIfMove(p: var PacketParser): ProgressState =
  assert p.bufRealLen == 0
  if p.bufLen > p.bufPos:
    assert p.remainingPayloadLen == 0
    move(p)
    return prgNext
  else: 
    if p.remainingPayloadLen > 0:
      return prgEmpty
    else:
      move(p)
      return prgEmpty

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
  dec(p.remainingPayloadLen, n)
  if p.want > 0:
    return checkIfMove(p)
  field = toProtocolInt(p.word)
  setLen(p.word, 0)

proc parseFixed(p: var PacketParser, buf: pointer, size: int): (ProgressState, bool) =
  if p.want == 0:
    return (prgOk, false)
  if p.bufRealLen == 0:
    return (checkIfMove(p), false)
  let want = p.want
  joinFixedStr(buf, size, p.want, offsetChar(p.buf, p.bufPos), p.bufRealLen)
  let n = want - p.want
  inc(p.bufPos, n)
  dec(p.bufRealLen, n)
  dec(p.remainingPayloadLen, n)
  if p.want > 0:
    if p.bufRealLen == 0:
      return (checkIfMove(p), false)
    else:
      return (prgOk, true)

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
  dec(p.remainingPayloadLen, n)
  if p.want > 0:
    return checkIfMove(p)

proc parseNul(p: var PacketParser, field: var string): ProgressState =
  result = prgOk
  if p.bufRealLen == 0:
    return checkIfMove(p)
  let (finished, count) = joinNulStr(field, offsetChar(p.buf, p.bufPos), p.bufRealLen)
  inc(p.bufPos, count)
  dec(p.bufRealLen, count)
  dec(p.remainingPayloadLen, count)
  if not finished:
    return checkIfMove(p)

proc parseFiller(p: var PacketParser): ProgressState =
  result = prgOk
  if p.want > p.bufRealLen:
    inc(p.bufPos, p.bufRealLen)
    dec(p.remainingPayloadLen, p.bufRealLen)
    dec(p.want, p.bufRealLen)
    dec(p.bufRealLen, p.bufRealLen)
    return checkIfMove(p)
  else:  
    let n = p.want
    inc(p.bufPos, n)
    dec(p.bufRealLen, n)
    dec(p.remainingPayloadLen, n)
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
        raise newException(ValueError, "bad encoded flag " & toProtocolHex(value, 1))  
      p.wantEncodedState = lenIntVal
    of lenIntVal:
      return parseFixed(p, field)
    else:
      raise newException(ValueError, "unexpected state " & $p.wantEncodedState)

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
      elif value == 0xFB: # 0xFB means that this string field is ``NULL``
        field = nil
        return prgOk
      elif value == 0xFC:
        p.want = 2
      elif value == 0xFD:
        p.want = 3
      elif value == 0xFE:
        p.want = 8
      else:
        raise newException(ValueError, "bad encoded flag " & toProtocolHex(value, 1))  
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
  of prgNext:
    continue
  of prgEmpty:
    return false

template checkIfOk(state: ProgressState): untyped =
  case state
  of prgOk:
    discard
  of prgNext:
    return prgNext
  of prgEmpty:
    return prgEmpty

proc parseHandshakeProgress(p: var PacketParser, packet: var HandshakePacket): ProgressState = 
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
      # if p.isEntire and p.remainingPayloadLen == 0:
      if p.remainingPayloadLen == 0:
        packet.sequenceId = p.sequenceId
        return prgOk
      else:  
        packet.state = hssPlugin
    of hssPlugin:
      # According to the docs this should be 0x00 terminated, but MariaDB does
      # not do this, so we assume this string to be packet terminated.
      checkIfOk parseNul(p, packet.plugin)
      packet.sequenceId = p.sequenceId
      return prgOk

proc parseHandshake*(p: var PacketParser, packet: var HandshakePacket): bool = 
  ## Parses the buffer data in ``buf``. ``size`` is the length of ``buf``.
  ## If parsing is complete, ``p``.``finished`` will be ``true``.
  while true:
    case p.state
    of packInit:
      packet = initHandshakePacket()
      p.state = packHandshake
      p.want = 1
      move(p)
    of packHeader:
      checkPrg parseHeader(p)
    of packHandshake:
      checkPrg parseHandshakeProgress(p, packet)
      p.state = packFinish
    of packFinish:
      return true
    else:
      raise newException(ValueError, "unexpected state " & $p.state)

proc parseResultHeader*(p: var PacketParser, packet: var ResultPacket): bool = 
  ## Parses the buffer data in ``buf``. ``size`` is the length of ``buf``.
  ## If parsing is complete, ``p``.``finished`` will be ``true``.
  while true:
    case p.state
    of packInit:
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
        packet = initResultPacket(rpkOk)
        p.state = packResultOk
        p.want = 1
        p.wantEncodedState = lenFlagVal
      of 0xFF:
        packet = initResultPacket(rpkError)
        p.state = packResultError
        p.want = 2
        p.wantEncodedState = lenFlagVal
      else:
        packet = initResultPacket(rpkResultSet)
        p.state = packResultSetFields
        p.want = p.remainingPayloadLen
    of packResultOk, packResultError, packResultSetFields:
      return true
    else:
      raise newException(ValueError, "unexpected state " & $p.state)

proc parseOkProgress(p: var PacketParser, packet: var ResultPacket, capabilities: int): ProgressState =
  template checkHowStatusInfo: untyped =
    if (capabilities and CLIENT_SESSION_TRACK) > 0 and p.remainingPayloadLen > 0:
      packet.okState = okStatusInfo
      p.want = 1
      p.wantEncodedState = lenFlagVal
    else:
      packet.okState = okStatusInfo
      p.want = p.remainingPayloadLen
  while true:
    case packet.okState
    of okAffectedRows:
      checkIfOk parseLenEncoded(p, packet.affectedRows)
      packet.okState = okLastInsertId
      p.want = 1
      p.wantEncodedState = lenFlagVal
    of okLastInsertId:
      checkIfOk parseLenEncoded(p, packet.lastInsertId)
      if (capabilities and CLIENT_PROTOCOL_41) > 0 or 
         (capabilities and CLIENT_TRANSACTIONS) > 0:
        packet.okState = okServerStatus
        p.want = 2
      else:
        checkHowStatusInfo
    of okServerStatus:
      checkIfOk parseFixed(p, packet.serverStatus)
      packet.hasMoreResults = (packet.serverStatus and SERVER_MORE_RESULTS_EXISTS) > 0
      if (capabilities and CLIENT_PROTOCOL_41) > 0:
        packet.okState = okWarningCount
        p.want = 2
      else:
        checkHowStatusInfo
    of okWarningCount:
      checkIfOk parseFixed(p, packet.warningCount)
      checkHowStatusInfo
    of okStatusInfo:
      if (capabilities and CLIENT_SESSION_TRACK) > 0 and p.remainingPayloadLen > 0:
        checkIfOk parseLenEncoded(p, packet.message)
        packet.okState = okSessionState
        p.want = 1
        p.wantEncodedState = lenFlagVal
      else:
        checkIfOk parseFixed(p, packet.message)
        packet.sequenceId = p.sequenceId
        return prgOk
    of okSessionState:
      checkIfOk parseLenEncoded(p, packet.sessionState)
      packet.sequenceId = p.sequenceId
      return prgOk

proc parseOk*(p: var PacketParser, packet: var ResultPacket, capabilities: int): bool =
  while true:
    case p.state
    of packHeader:
      checkPrg parseHeader(p)
    of packResultOk:
      checkPrg parseOkProgress(p, packet, capabilities)
      p.state = packFinish
    of packFinish:
      return true
    else:
      raise newException(ValueError, "unexpected state " & $p.state)  

proc parseErrorProgress(p: var PacketParser, packet: var ResultPacket, capabilities: int): ProgressState =
  while true:
    case packet.errState
    of errErrorCode:
      checkIfOk parseFixed(p, packet.errorCode)
      if (capabilities and CLIENT_PROTOCOL_41) > 0:
        packet.errState = errSqlStateMarker
        p.want = 1
      else:
        packet.errState = errErrorMessage
        p.want = p.remainingPayloadLen
    of errSqlStateMarker:
      checkIfOk parseFixed(p, packet.sqlStateMarker)
      packet.errState = errSqlState
      p.want = 5
    of errSqlState:
      checkIfOk parseFixed(p, packet.sqlState)
      packet.errState = errErrorMessage
      p.want = p.remainingPayloadLen
    of errErrorMessage:
      checkIfOk parseFixed(p, packet.errorMessage)
      packet.sequenceId = p.sequenceId
      return prgOk

proc parseError*(p: var PacketParser, packet: var ResultPacket, capabilities: int): bool =
  while true:
    case p.state
    of packHeader:
      checkPrg parseHeader(p)
    of packResultError:
      checkPrg parseErrorProgress(p, packet, capabilities)
      p.state = packFinish
    of packFinish:
      return true
    else:
      raise newException(ValueError, "unexpected state " & $p.state)  

proc parseEofProgress(p: var PacketParser, packet: var EofPacket, capabilities: int): ProgressState =
  while true:
    case packet.state
    of eofHeader:
      if (capabilities and CLIENT_PROTOCOL_41) > 0:
        packet.state = eofWarningCount
        p.want = 2
      else:
        assert p.remainingPayloadLen == 0
        return prgOk
    of eofWarningCount:
      checkIfOk parseFixed(p, packet.warningCount)
      packet.state = eofServerStatus
      p.want = 2
    of eofServerStatus:
      checkIfOk parseFixed(p, packet.serverStatus)
      assert p.remainingPayloadLen == 0
      return prgOk

proc parseFieldProgress(p: var PacketParser, packet: var FieldPacket, capabilities: int): ProgressState =
  while true:
    case packet.state
    of fieldCatalog:
      if (capabilities and CLIENT_PROTOCOL_41) > 0:
        checkIfOk parseFixed(p, packet.catalog)
        packet.state = fieldSchema
        p.want = 1
        p.wantEncodedState = lenFlagVal
      else:
        packet.state = fieldTable
        # p.want = 1
        # p.wantEncodedState = lenFlagVal
    of fieldSchema:
      checkIfOk parseLenEncoded(p, packet.schema)
      packet.state = fieldTable
      p.want = 1
      p.wantEncodedState = lenFlagVal
    of fieldTable:
      #checkIfOk parseLenEncoded(p, packet.table)
      if (capabilities and CLIENT_PROTOCOL_41) > 0:
        checkIfOk parseLenEncoded(p, packet.table)
        packet.state = fieldOrgTable
        p.want = 1
        p.wantEncodedState = lenFlagVal
      else:
        checkIfOk parseFixed(p, packet.table)
        packet.state = fieldName
        p.want = 1
        p.wantEncodedState = lenFlagVal
    of fieldOrgTable:
      checkIfOk parseLenEncoded(p, packet.orgTable)
      packet.state = fieldName
      p.want = 1
      p.wantEncodedState = lenFlagVal
    of fieldName:
      checkIfOk parseLenEncoded(p, packet.name)
      if (capabilities and CLIENT_PROTOCOL_41) > 0:
        packet.state = fieldOrgName
        p.want = 1
        p.wantEncodedState = lenFlagVal
      else:
        packet.state = fieldLen
        p.want = 4
    of fieldOrgName:
      checkIfOk parseLenEncoded(p, packet.orgName)
      packet.state = fieldFiller1
      p.want = 1
      p.wantEncodedState = lenFlagVal
    of fieldFiller1:
      var fieldsLen: int
      checkIfOk parseLenEncoded(p, fieldsLen)
      assert fieldsLen == 0x0c
      packet.state = fieldCharset
      p.want = 2
    of fieldCharset:
      checkIfOk parseFixed(p, packet.charset)
      packet.state = fieldLen
      p.want = 4
    of fieldLen:
      checkIfOk parseFixed(p, packet.fieldLen)  
      packet.state = fieldType
      if (capabilities and CLIENT_PROTOCOL_41) > 0:
        p.want = 1
      else:
        p.want = 2
    of fieldType:
      checkIfOk parseFixed(p, packet.fieldType)
      packet.state = fieldFlags
      p.want = 2
    of fieldFlags:
      checkIfOk parseFixed(p, packet.fieldFlags)
      packet.state = fieldDecimals
      p.want = 1
    of fieldDecimals:
      checkIfOk parseFixed(p, packet.decimals)
      if (capabilities and CLIENT_PROTOCOL_41) > 0:
        packet.state = fieldFiller2
        p.want = 2
      else:
        packet.state = fieldDefaultValue
        p.want = p.remainingPayloadLen
    of fieldFiller2:
      checkIfOk parseFiller(p)
      if p.command == COM_FIELD_LIST: 
        packet.state = fieldDefaultValue
        p.want = 1
        p.wantEncodedState = lenFlagVal
      else:
        assert p.remainingPayloadLen == 0
        return prgOk
    of fieldDefaultValue:
      checkIfOk parseLenEncoded(p, packet.defaultValue)
      assert p.remainingPayloadLen == 0
      return prgOk

proc parseFields*(p: var PacketParser, packet: var ResultPacket, capabilities: int): bool =
  template checkIfOk(state: ProgressState): untyped =
    case state
    of prgOk:
      discard
    of prgNext:
      break
    of prgEmpty:
      return false
  while true:
    case p.state
    of packHeader:
      checkPrg parseHeader(p)
    of packResultSetFields:
      while true:
        case packet.rsetState
        of rsetExtra:
          if p.want > 0:
            checkIfOk parseFixed(p, packet.extra)
          p.want = 1
          packet.rsetState = rsetFieldHeader
        of rsetFieldHeader: 
          var header: int
          checkIfOk parseFixed(p, header)
          if header == 0xFE and p.payloadLen < 9:
            packet.rsetState = rsetFieldEof
            p.want = 1
          else:
            var field = initFieldPacket()
            add(packet.fields, field)
            packet.rsetState = rsetField
            p.want = header
        of rsetField:
          checkIfOk parseFieldProgress(p, packet.fields[packet.fieldsPos], capabilities)
          packet.rsetState = rsetFieldHeader
          p.want = 1
          inc(packet.fieldsPos)
        of rsetFieldEof:
          checkIfOk parseEofProgress(p, packet.fieldsEof, capabilities) 
          if p.command == COM_FIELD_LIST:
            packet.hasMoreResults = (packet.fieldsEof.serverStatus and SERVER_MORE_RESULTS_EXISTS) > 0
            packet.sequenceId = p.sequenceId
            packet.hasRows = false
            p.state = packFinish
            break
          else:
            packet.rsetState = rsetRowHeader
            packet.hasRows = true
            p.state = packResultSetRows
            p.want = 1
            p.wantEncodedState = lenFlagVal 
            break
        else:
          raise newException(ValueError, "unexpected state " & $packet.rsetState) 
    of packResultSetRows:
      return true
    of packFinish:
      return true
    else:
      raise newException(ValueError, "unexpected state " & $p.state) 

proc parseRows*(p: var PacketParser, packet: var ResultPacket, capabilities: int, 
                buf: pointer, size: int): tuple[offset: int, state: RowsState] =
  template checkIfOk(state: ProgressState): untyped =
    case state
    of prgOk:
      discard
    of prgNext:
      break
    of prgEmpty:
      return (0, rowsBufEmpty)
  while true:
    case p.state
    of packHeader:
      let prgState = parseHeader(p)
      case prgState
      of prgOk:
        discard
      of prgNext:
        continue
      of prgEmpty:
        return (0, rowsBufEmpty)
    of packResultSetRows:
      while true:
        case packet.rsetState
        of rsetRowHeader: 
          var header: int
          checkIfOk parseFixed(p, header)
          if header == 0xFE and p.payloadLen < 9:
            packet.rsetState = rsetRowEof
            p.want = 1
          elif header == 0xFB:
            packet.rsetState = rsetRow
            packet.meetNull = true
            return (0, rowsFieldBegin)
          else:
            packet.rsetState = rsetRow
            p.want = header
            assert p.want > 0
            return (0, rowsFieldBegin)
        of rsetRow:
          if packet.meetNull:
            assert size > 0
            packet.meetNull = false
            cast[ptr char](buf)[] = '\0' # NULL ==> '\0' 
            packet.rsetState = rsetRowHeader
            p.want = 1
            p.wantEncodedState = lenFlagVal 
            return (1, rowsFieldEnd)
          else:  
            let w = p.want
            let (prgState, full) = parseFixed(p, buf, size)
            let offset = w - p.want
            if full:
              return (offset, rowsFieldFull)
            case prgState
            of prgOk:
              packet.rsetState = rsetRowHeader
              p.want = 1
              p.wantEncodedState = lenFlagVal 
              return (offset, rowsFieldEnd)
            of prgNext:
              break
            of prgEmpty:
              return (offset, rowsBufEmpty)
        of rsetRowEof:
          checkIfOk parseEofProgress(p, packet.rowsEof, capabilities)
          packet.hasMoreResults = (packet.rowsEof.serverStatus and SERVER_MORE_RESULTS_EXISTS) > 0
          packet.sequenceId = p.sequenceId
          p.state = packFinish
          break
        else:
          raise newException(ValueError, "unexpected state " & $packet.rsetState) 
    of packFinish:
      return (0, rowsFinished)
    else:
      raise newException(ValueError, "unexpected state " & $p.state)

proc parseRows*(p: var PacketParser, packet: var ResultPacket, capabilities: int, 
                rows: var RowList): bool =
  template checkIfOk(state: ProgressState): untyped =
    case state
    of prgOk:
      discard
    of prgNext:
      break
    of prgEmpty:
      return false
  while true:
    case p.state
    of packHeader:
      checkPrg parseHeader(p)
    of packResultSetRows:
      while true:
        case packet.rsetState
        of rsetRowHeader: 
          var header: int
          checkIfOk parseFixed(p, header)
          if header == 0xFE and p.payloadLen < 9:
            packet.rsetState = rsetRowEof
            p.want = 1
          elif header == 0xFB:
            packet.rsetState = rsetRow
            packet.meetNull = true
            inc(rows.counter)
            add(rows.value, newStringOfCap(1))
          else:
            packet.rsetState = rsetRow
            p.want = header
            inc(rows.counter)
            add(rows.value, newStringOfCap(header))
        of rsetRow:
          if packet.meetNull:
            packet.meetNull = false
            rows.value[rows.counter] = nil # NULL ==> nil
            packet.rsetState = rsetRowHeader
            p.want = 1
            p.wantEncodedState = lenFlagVal 
          else: 
            checkIfOk parseFixed(p, rows.value[rows.counter])
            packet.rsetState = rsetRowHeader
            p.want = 1
            p.wantEncodedState = lenFlagVal 
        of rsetRowEof:
          checkIfOk parseEofProgress(p, packet.rowsEof, capabilities)
          packet.hasMoreResults = (packet.rowsEof.serverStatus and SERVER_MORE_RESULTS_EXISTS) > 0
          packet.sequenceId = p.sequenceId
          p.state = packFinish
          break
        else:
          raise newException(ValueError, "unexpected state " & $packet.rsetState) 
    of packFinish:
      return true
    else:
      raise newException(ValueError, "unexpected state " & $p.state)

type
  ClientAuthenticationPacket* = object ## The authentication packet for the handshaking connection.
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

  ChangeUserPacket* = object ## The packet for the change user command.
    ## Packet for change user.
    sequenceId*: int           # 1
    user*: string              # NullTerminatedString
    # scrambleLen              # 1
    scrambleBuff*: string      # 
    database*: string          # NullTerminatedString
    charset*: int              # [1]

proc parseHex(c: char): int =
  case c
  of '0'..'9':
    result = ord(c.toUpperAscii) - ord('0') 
  of 'a'..'f':
    result = ord(c.toUpperAscii) - ord('A') + 10
  of 'A'..'F':
    result = ord(c.toUpperAscii) - ord('A') + 10
  else:
    raise newException(ValueError, "unexpected hex char " & c)

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

proc formatClientAuth*(packet: ClientAuthenticationPacket, password: string): string = 
  ## Converts ``packet`` to a string.
  if packet.protocol41:
    let payloadLen = 4 + 4 + 1 + 23 + (packet.user.len + 1) + (1 + 20) + 
                     (packet.database.len + 1)
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
    let payloadLen = 2 + 3 + (packet.user.len + 1) + 
                     8 + 1 + (packet.database.len + 1)
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
  ## Converts to a ``COM_QUIT`` (mysql Client/Server Protocol) string.
  formatNoArgsComImpl COM_QUIT

proc formatComInitDb*(database: string): string = 
  ## Converts to a ``COM_INIT_DB`` (mysql Client/Server Protocol) string.
  formatRestStrComImpl COM_INIT_DB, database

proc formatComQuery*(sql: string): string = 
  ## Converts to a ``COM_QUERY`` (mysql Client/Server Protocol) string.
  formatRestStrComImpl COM_QUERY, sql

proc formatComChangeUser*(packet: ChangeUserPacket, password: string): string = 
  ## Converts to a ``COM_CHANGE_USER`` (mysql Client/Server Protocol) string.
  let payloadLen = 1 + (packet.user.len + 1) + (1 + 20) + (packet.database.len + 1) + 2 
  result = newStringOfCap(4 + payloadLen)
  add(result, toProtocolHex(payloadLen, 3))
  add(result, toProtocolHex(0, 1))
  add(result, toProtocolHex(COM_CHANGE_USER.int, 1))
  add(result, packet.user)
  add(result, '\0')
  add(result, toProtocolHex(20, 1))
  add(result, token(packet.scrambleBuff, password))
  add(result, packet.database)
  add(result, '\0')
  add(result, toProtocolHex(packet.charset, 2))

proc formatComPing*(): string = 
  ## Converts to a ``COM_PING`` (mysql Client/Server Protocol) string.
  formatNoArgsComImpl COM_PING
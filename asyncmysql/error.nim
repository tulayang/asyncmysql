#    AsyncMysql - Asynchronous MySQL connector written in pure Nim
#        (c) Copyright 2017 Wang Tong
#
#    See the file "LICENSE", included in this distribution, for
#    details about the copyright.

## This module is mainly used for error handling.

type
  MysqlError* = object of Exception ## Raised if a mysql operation failed.

proc raiseMysqlError*(msg: string) =
  raise newException(MysqlError, msg)
#    AsyncMysql - Asynchronous MySQL connector written in pure Nim
#        (c) Copyright 2017 Wang Tong
#
#    See the file "LICENSE", included in this distribution, for
#    details about the copyright.

import asyncdispatch, asyncnet, net, strutils

proc waitFor1*(fut: Future[void]) =
  proc check() {.async.} =
    try:
      await fut
    except:
      let err = getCurrentException()
      echo " [", err.name, "] unhandled exception: ", err.msg 
      quit(QuitFailure)
  waitFor check()

proc echoHex*(messageHeader: string, s: string) =
  write(stdout, messageHeader)
  for c in s:
    write(stdout, toHex(ord(c), 2), ' ')
  write(stdout, '\L')
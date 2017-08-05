#
#
#            Nim's Runtime Library
#        (c) Copyright 2015 Andreas Rumpf
#
#

type
  SqlQuery* = distinct string ## An SQL query string

proc dbQuote(s: string): string =
  ## DB quotes the string.
  result = "'"
  for c in items(s):
    if c == '\'': add(result, "''")
    if c == '\\': add(result, "\\\\")
    else: add(result, c)
  add(result, '\'')

proc dbFormat(formatstr: string, args: varargs[string]): string =
  result = ""
  var a = 0
  for c in items(formatstr):
    if c == '?':
      if args[a] == nil:
        add(result, "NULL")
      else:
        add(result, dbQuote(args[a]))
      inc(a)
    else:
      add(result, c)

template sql*(query: string, args: varargs[string, `$`]): SqlQuery =
  ## constructs a SqlQuery from the string `query`. 
  ##
  ## If assertions are turned off, it does nothing. If assertions are turned
  ## on, later versions will check the string for valid syntax.
  SqlQuery(dbFormat(query, args))
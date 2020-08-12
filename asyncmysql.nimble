# Package
version = "0.4.4"
author = "Wang Tong"
description = "Asynchronous MySQL connector written in pure Nim."
license = "MIT"
skipFiles = @["TODO.md"]

# Dependencies
requires "nim >= 0.19.0", "mysqlparser >= 0.2.0"

import ospaths, strutils

proc reGenDoc(filename: string) =
  writeFile(filename,
            replace(
              replace(readFile(filename), 
                      """href="/tree/master/asyncmysql""", 
                      """href="https://github.com/tulayang/asyncmysql/tree/master/asyncmysql""" ),
              """href="/edit/devel/asyncmysql""",
              """href="https://github.com/tulayang/asyncmysql/edit/master/asyncmysql""" ))


task doc, "Generate documentation":
  for name in [
    "error",  "query", "connection", "pool"
  ]:
    exec "nim doc2 -o:$outfile --docSeeSrcUrl:$url $file" % [
      "outfile", thisDir() / "doc" / name & ".html",
      "url",     "https://github.com/tulayang/asyncmysql/blob/master",
      "file",    thisDir() / "asyncmysql" / name & ".nim"
    ]
    reGenDoc thisDir() / "doc" / name & ".html"
  exec "nim rst2html -o:$outfile $file" % [
    "outfile", thisDir() / "doc" / "index.html",
    "file",    thisDir() / "doc" / "index.rst"
  ]
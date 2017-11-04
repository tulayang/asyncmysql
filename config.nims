import ospaths, strutils

proc reGenDoc(filename: string) =
  writeFile(filename,
            replace(
              replace(readFile(filename), 
                      """href="/tree/master/asyncmysql""", 
                      """href="https://github.com/tulayang/asyncmysql/tree/master/asyncmysql""" ),
              """href="/edit/devel/asyncmysql""",
              """href="https://github.com/tulayang/asyncmysql/edit/master/asyncmysql""" ))

template runTest(name: string) =
  withDir thisDir():
    mkDir "bin"
    --r
    --o:"""bin/""" name
    --verbosity:0
    --path:"""."""
    setCommand "c", "test/" & name & ".nim"

template runBenchmark(name: string) =
  withDir thisDir():
    mkDir "bin"
    --r
    --o:"""bin/""" name
    --verbosity:0
    setCommand "c", "benchmark/" & name & ".nim"

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

task test, "Run test tests":
  runTest "test"

task test_connection, "Run test connection tests":
  runTest "test_connection"

task test_pool, "Run test pool tests":
  runTest "test_pool"


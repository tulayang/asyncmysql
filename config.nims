import ospaths, strutils

proc reGenDoc(filename: string) =
  writeFile(filename,
            replace(
              replace(readFile(filename), 
                      """href="/tree/master/asyncmysql""", 
                      """href="https://github.com/tulayang/nimnode/tree/master/asyncmysql""" ),
              """href="/edit/devel/asyncmysql""",
              """href="https://github.com/tulayang/nimnode/edit/master/asyncmysql""" ))

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

task test, "Run test tests":
  runTest "test"


import mysqlparser, error, query, macros

macro asyncRecv*(conn: untyped, kind: untyped): untyped =
  var prc:string
  var prcIdent:NimNode
  if eqIdent(kind, "rpkOk"):
    prc = "parseOk"
  elif eqIdent(kind,"rpkError"):
    prc = "parseError"
  elif eqIdent(kind,"rpkResultSet"):
    prc = "parseFields"
  prcIdent = ident(prc)
  result = nnkStmtList.newTree(
    nnkVarSection.newTree(
      nnkIdentDefs.newTree(
        newIdentNode("finished"),
        newEmptyNode(),
        newIdentNode("false")
      )
    ),
    nnkIfStmt.newTree(
      nnkElifBranch.newTree(
        nnkDotExpr.newTree(
          nnkDotExpr.newTree(
            conn,
            newIdentNode("parser")
          ),
          newIdentNode("buffered")
        ),
        nnkStmtList.newTree(
          nnkAsgn.newTree(
            newIdentNode("finished"),
            nnkCall.newTree(
              prcIdent,
              nnkDotExpr.newTree(
                conn,
                newIdentNode("parser")
              ),
              nnkDotExpr.newTree(
                conn,
                newIdentNode("resultPacket")
              ),
              nnkDotExpr.newTree(
                nnkDotExpr.newTree(
                  conn,
                  newIdentNode("handshakePacket")
                ),
                newIdentNode("capabilities")
              )
            )
          )
        )
      )
    ),
    nnkIfStmt.newTree(
      nnkElifBranch.newTree(
        nnkPrefix.newTree(
          newIdentNode("not"),
          newIdentNode("finished")
        ),
        nnkStmtList.newTree(
          nnkWhileStmt.newTree(
            newIdentNode("true"),
            nnkStmtList.newTree(
              nnkYieldStmt.newTree(
                nnkCall.newTree(
                  newIdentNode("recv"),
                  conn
                )
              ),
              nnkCall.newTree(
                newIdentNode("mount"),
                nnkDotExpr.newTree(
                  conn,
                  newIdentNode("parser")
                ),
                nnkDotExpr.newTree(
                  nnkBracketExpr.newTree(
                    nnkDotExpr.newTree(
                      conn,
                      newIdentNode("buf")
                    ),
                    nnkDotExpr.newTree(
                      conn,
                      newIdentNode("bufPos")
                    )
                  ),
                  newIdentNode("addr")
                ),
                nnkDotExpr.newTree(
                  conn,
                  newIdentNode("bufLen")
                )
              ),
              nnkAsgn.newTree(
                newIdentNode("finished"),
                nnkCall.newTree(
                  prcIdent,
                  nnkDotExpr.newTree(
                    conn,
                    newIdentNode("parser")
                  ),
                  nnkDotExpr.newTree(
                    conn,
                    newIdentNode("resultPacket")
                  ),
                  nnkDotExpr.newTree(
                    nnkDotExpr.newTree(
                      conn,
                      newIdentNode("handshakePacket")
                    ),
                    newIdentNode("capabilities")
                  )
                )
              ),
              nnkIfStmt.newTree(
                nnkElifBranch.newTree(
                  newIdentNode("finished"),
                  nnkStmtList.newTree(
                    nnkBreakStmt.newTree(
                      newEmptyNode()
                    )
                  )
                )
              )
            )
          )
        )
      )
    )
  )
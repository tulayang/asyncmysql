
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
            newIdentNode("conn"),
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
                newIdentNode("conn"),
                newIdentNode("parser")
              ),
              nnkDotExpr.newTree(
                newIdentNode("conn"),
                newIdentNode("resultPacket")
              ),
              nnkDotExpr.newTree(
                nnkDotExpr.newTree(
                  newIdentNode("conn"),
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
              nnkCommand.newTree(
                newIdentNode("yield"),
                nnkCall.newTree(
                  newIdentNode("recv"),
                  newIdentNode("conn")
                )
              ),
              nnkCall.newTree(
                newIdentNode("mount"),
                nnkDotExpr.newTree(
                  newIdentNode("conn"),
                  newIdentNode("parser")
                ),
                nnkDotExpr.newTree(
                  nnkBracketExpr.newTree(
                    nnkDotExpr.newTree(
                      newIdentNode("conn"),
                      newIdentNode("buf")
                    ),
                    nnkDotExpr.newTree(
                      newIdentNode("conn"),
                      newIdentNode("bufPos")
                    )
                  ),
                  newIdentNode("addr")
                ),
                nnkDotExpr.newTree(
                  newIdentNode("conn"),
                  newIdentNode("bufLen")
                )
              ),
              nnkAsgn.newTree(
                newIdentNode("finished"),
                nnkCall.newTree(
                  prcIdent,
                  nnkDotExpr.newTree(
                    newIdentNode("conn"),
                    newIdentNode("parser")
                  ),
                  nnkDotExpr.newTree(
                    newIdentNode("conn"),
                    newIdentNode("resultPacket")
                  ),
                  nnkDotExpr.newTree(
                    nnkDotExpr.newTree(
                      newIdentNode("conn"),
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
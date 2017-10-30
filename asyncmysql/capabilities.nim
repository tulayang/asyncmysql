#    AsyncMysql - Asynchronous MySQL connector written in pure Nim
#        (c) Copyright 2017 Wang Tong
#
#    See the file "LICENSE", included in this distribution, for
#    details about the copyright.

## This module provides client capabilities which are used by MySQL Client/Server Protocol to 
## affect the connection's behavior.

import mysqlparser

when defined(asyncmysqldoc):
  const 
    CLIENT_LONG_PASSWORD* = CLIENT_LONG_PASSWORD 
      ## Use the improved version of Old Password Authentication.
    CLIENT_FOUND_ROWS* = CLIENT_FOUND_ROWS 
      ## Send found rows instead of affected rows in EOF_Packet.
    CLIENT_LONG_FLAG* = CLIENT_LONG_FLAG 
      ## Get all field flags.
    CLIENT_CONNECT_WITH_DB* = CLIENT_CONNECT_WITH_DB 
      ## Database (schema) name can be specified on connect in Handshake Response Packet.
    CLIENT_NO_SCHEMA* = CLIENT_NO_SCHEMA 
      ## Don't allow database.table.field.
    CLIENT_COMPRESS* = CLIENT_COMPRESS  
      ## Compression protocol supported. 
    CLIENT_ODBC* = CLIENT_ODBC 
      ## Special handling of ODBC behavior. 
    CLIENT_LOCAL_FILES* = CLIENT_LOCAL_FILES 
      ## Can use LOAD DATA LOCAL.
    CLIENT_IGNORE_SPACE* = CLIENT_IGNORE_SPACE  
      ## Ignore spaces before '('.
    CLIENT_PROTOCOL_41* = CLIENT_PROTOCOL_41 
      ## New 4.1 protocol.
    CLIENT_INTERACTIVE* = CLIENT_INTERACTIVE 
      ## This is an interactive client.
    CLIENT_SSL* = CLIENT_SSL 
      ## Use SSL encryption for the session.
    CLIENT_IGNORE_SIGPIPE* = CLIENT_IGNORE_SIGPIPE  
      ## Client only flag. 
    CLIENT_TRANSACTIONS* = CLIENT_TRANSACTIONS 
      ## Client knows about transactions.
    CLIENT_RESERVED* = CLIENT_RESERVED  
      ## DEPRECATED: Old flag for 4.1 protocol.
    CLIENT_RESERVED2* = CLIENT_RESERVED2 
      ## DEPRECATED: Old flag for 4.1 authentication.
    CLIENT_MULTI_STATEMENTS* = CLIENT_MULTI_STATEMENTS 
      ## Enable/disable multi-stmt support.
    CLIENT_MULTI_RESULTS* = CLIENT_MULTI_RESULTS 
      ## Enable/disable multi-results.
    CLIENT_PS_MULTI_RESULTS* = CLIENT_PS_MULTI_RESULTS 
      ## Multi-results and OUT parameters in PS-protocol. 
    CLIENT_PLUGIN_AUTH* = CLIENT_PLUGIN_AUTH 
      ## Client supports plugin authentication.
    CLIENT_CONNECT_ATTRS* = CLIENT_CONNECT_ATTRS 
      ## Client supports connection attributes.
    CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA* = CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA  
      ## Enable authentication response packet to be larger than 255 bytes.
    CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS* = CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS 
      ## Don't close the connection for a user account with expired password.
    CLIENT_SESSION_TRACK* = CLIENT_SESSION_TRACK 
      ## Capable of handling server state change information. 
    CLIENT_DEPRECATE_EOF* = CLIENT_DEPRECATE_EOF 
      ## Client no longer needs EOF_Packet and will use OK_Packet instead.
    CLIENT_SSL_VERIFY_SERVER_CERT* = CLIENT_SSL_VERIFY_SERVER_CERT  
      ## Verify server certificate.
    CLIENT_REMEMBER_OPTIONS* = CLIENT_REMEMBER_OPTIONS 
      ## Don't reset the options after an unsuccessful connect.
else:
  export 
    CLIENT_LONG_PASSWORD, ## Use the improved version of Old Password Authentication.
    CLIENT_FOUND_ROWS, ## Send found rows instead of affected rows in EOF_Packet.
    CLIENT_LONG_FLAG, ## Get all field flags.
    CLIENT_CONNECT_WITH_DB, ## Database (schema) name can be specified on connect in Handshake Response Packet.
    CLIENT_NO_SCHEMA, ## Don't allow database.table.field.
    CLIENT_COMPRESS,  ## Compression protocol supported. 
    CLIENT_ODBC, ## Special handling of ODBC behavior. 
    CLIENT_LOCAL_FILES, ## Can use LOAD DATA LOCAL.
    CLIENT_IGNORE_SPACE,  ## Ignore spaces before '('.
    CLIENT_PROTOCOL_41, ## New 4.1 protocol.
    CLIENT_INTERACTIVE, ## This is an interactive client.
    CLIENT_SSL, ## Use SSL encryption for the session.
    CLIENT_IGNORE_SIGPIPE,  ## Client only flag. 
    CLIENT_TRANSACTIONS, ## Client knows about transactions.
    CLIENT_RESERVED,  ## DEPRECATED: Old flag for 4.1 protocol.
    CLIENT_RESERVED2, ## DEPRECATED: Old flag for 4.1 authentication.
    CLIENT_MULTI_STATEMENTS, ## Enable/disable multi-stmt support.
    CLIENT_MULTI_RESULTS, ## Enable/disable multi-results.
    CLIENT_PS_MULTI_RESULTS, ## Multi-results and OUT parameters in PS-protocol. 
    CLIENT_PLUGIN_AUTH ,## Client supports plugin authentication.
    CLIENT_CONNECT_ATTRS, ## Client supports connection attributes.
    CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA,  ## Enable authentication response packet to be larger than 255 bytes.
    CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS, ## Don't close the connection for a user account with expired password.
    CLIENT_SESSION_TRACK, ## Capable of handling server state change information. 
    CLIENT_DEPRECATE_EOF, ## Client no longer needs EOF_Packet and will use OK_Packet instead.
    CLIENT_SSL_VERIFY_SERVER_CERT,  ## Verify server certificate.
    CLIENT_REMEMBER_OPTIONS ## Don't reset the options after an unsuccessful connect.
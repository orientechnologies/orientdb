/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.enterprise.channel.binary;

/**
 * The range of the requests is 1-79.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OChannelBinaryProtocol {
  // OUTGOING
  public static final byte REQUEST_SHUTDOWN = 1;
  public static final byte REQUEST_CONNECT = 2;
  public static final byte REQUEST_HANDSHAKE = 20;

  public static final byte REQUEST_DB_OPEN = 3;
  public static final byte REQUEST_DB_CREATE = 4;
  public static final byte REQUEST_DB_CLOSE = 5;
  public static final byte REQUEST_DB_EXIST = 6;
  public static final byte REQUEST_DB_DROP = 7;
  public static final byte REQUEST_DB_SIZE = 8;
  public static final byte REQUEST_DB_COUNTRECORDS = 9;
  public static final byte REQUEST_DB_REOPEN = 17;

  public static final byte REQUEST_CLUSTER_ADD = 10;
  public static final byte REQUEST_CLUSTER_DROP = 11;
  public static final byte REQUEST_CLUSTER_COUNT = 12;
  public static final byte REQUEST_CLUSTER_DATARANGE = 13;

  public static final byte REQUEST_INCREMENTAL_BACKUP = 27; // since 2.2

  public static final byte REQUEST_RECORD_METADATA = 29; // since 1.4.0
  public static final byte REQUEST_RECORD_LOAD = 30;
  public static final byte REQUEST_RECORD_CREATE = 31;
  public static final byte REQUEST_RECORD_UPDATE = 32;
  public static final byte REQUEST_RECORD_DELETE = 33;
  public static final byte REQUEST_BATCH_OPERATIONS = 35; // since 3.0
  public static final byte REQUEST_POSITIONS_HIGHER = 36; // since 1.3.0
  public static final byte REQUEST_POSITIONS_LOWER = 37; // since 1.3.0
  public static final byte REQUEST_RECORD_CLEAN_OUT = 38; // since 1.3.0
  public static final byte REQUEST_POSITIONS_FLOOR = 39; // since 1.3.0

  public static final byte REQUEST_COUNT = 40; // DEPRECATED: USE
  // REQUEST_CLUSTER_COUNT

  public static final byte REQUEST_COMMAND = 41;
  public static final byte REQUEST_POSITIONS_CEILING = 42; // since 1.3.0
  public static final byte REQUEST_RECORD_HIDE = 43; // since 1.7
  public static final byte REQUEST_RECORD_LOAD_IF_VERSION_NOT_LATEST = 44; // since 2.1
  public static final byte REQUEST_QUERY = 45; // since 3.0
  public static final byte REQUEST_CLOSE_QUERY = 46; // since 3.0
  public static final byte REQUEST_QUERY_NEXT_PAGE = 47; // since 3.0

  public static final byte REQUEST_SERVER_QUERY = 50; // since 3.2

  public static final byte REQUEST_TX_COMMIT = 60;
  public static final byte REQUEST_TX_BEGIN = 61;
  public static final byte REQUEST_TX_REBEGIN = 62;
  public static final byte REQUEST_TX_FETCH = 63;

  public static final byte REQUEST_TX_ROLLBACK = 64;

  public static final byte REQUEST_CONFIG_GET = 70;
  public static final byte REQUEST_CONFIG_SET = 71;
  public static final byte REQUEST_CONFIG_LIST = 72;
  public static final byte REQUEST_DB_RELOAD = 73; // SINCE 1.0rc4
  public static final byte REQUEST_DB_LIST = 74; // SINCE 1.0rc6
  public static final byte REQUEST_SERVER_INFO = 75; // SINCE 2.2.0

  public static final byte REQUEST_OK_PUSH = 90;

  // DISTRIBUTED
  public static final byte REQUEST_CLUSTER = 92; // SINCE 1.0
  // Lock + sync
  public static final byte REQUEST_DB_FREEZE = 94; // SINCE 1.1.0
  public static final byte REQUEST_DB_RELEASE = 95; // SINCE 1.1.0

  // IMPORT
  public static final byte REQUEST_DB_IMPORT = 98;

  public static final byte SUBSCRIBE_PUSH = 100;
  public static final byte UNSUBSCRIBE_PUSH = 101;
  public static final byte EXPERIMENTAL = 102;

  // REMOTE SB-TREE COLLECTIONS
  public static final byte REQUEST_CREATE_SBTREE_BONSAI = 110;
  public static final byte REQUEST_SBTREE_BONSAI_GET = 111;
  public static final byte REQUEST_SBTREE_BONSAI_FIRST_KEY = 112;
  public static final byte REQUEST_SBTREE_BONSAI_GET_ENTRIES_MAJOR = 113;
  public static final byte REQUEST_RIDBAG_GET_SIZE = 114;

  // TASK
  public static final byte DISTRIBUTED_REQUEST = 120;
  public static final byte DISTRIBUTED_RESPONSE = 121;
  public static final byte DISTRIBUTED_CONNECT = 122;
  public static final byte COORDINATED_DISTRIBUTED_MESSAGE = 123;

  // INCOMING
  public static final byte RESPONSE_STATUS_OK = 0;
  public static final byte RESPONSE_STATUS_ERROR = 1;
  public static final byte PUSH_DATA = 3;

  // CONSTANTS
  public static final short RECORD_NULL = -2;
  public static final short RECORD_RID = -3;

  // FOR MORE INFO:
  // https://github.com/orientechnologies/orientdb/wiki/Network-Binary-Protocol#wiki-Compatibility
  public static final int PROTOCOL_VERSION_26 = 26;
  public static final int PROTOCOL_VERSION_27 = 27;
  public static final int PROTOCOL_VERSION_28 = 28; // SENT AS SHORT AS FIRST PACKET AFTER
  // SOCKET CONNECTION
  public static final int PROTOCOL_VERSION_29 = 29; // ADDED PUSH SUPPORT FOR LIVE QUERY
  public static final int PROTOCOL_VERSION_30 = 30; // NEW COMMAND TO READ RECORD ONLY IF
  // VERSION IS NOT LATEST WAS ADD
  public static final int PROTOCOL_VERSION_31 = 31; // CHANGED STORAGE CFG TO ADD
  // ENCRYPTION
  public static final int PROTOCOL_VERSION_32 = 32; // STREAMABLE RESULT SET

  public static final int PROTOCOL_VERSION_33 = 33; // INCREMENTAL BACKUP/RESTORE
  public static final int PROTOCOL_VERSION_34 = 34; // ON CONNECT DECLARE IF PUSH MESSAGES
  // ARE SUPPORTED + COLLECT STATS

  public static final int PROTOCOL_VERSION_35 = 35;
  public static final int PROTOCOL_VERSION_36 =
      36; // ABILITY TO CREATE DATABASE FROM INCREMENTAL BACKUP
  public static final int PROTOCOL_VERSION_37 = 37;
  public static final int PROTOCOL_VERSION_38 = 38;

  public static final int CURRENT_PROTOCOL_VERSION = PROTOCOL_VERSION_38;
  public static final int OLDEST_SUPPORTED_PROTOCOL_VERSION = PROTOCOL_VERSION_26;

  // This are specific messages inside the subscribe message
  public static final byte SUBSCRIBE_PUSH_DISTRIB_CONFIG = 1;

  public static final byte SUBSCRIBE_PUSH_LIVE_QUERY = 2;
  public static final byte UNSUBSCRIBE_PUSH_LIVE_QUERY = 2;

  public static final byte SUBSCRIBE_PUSH_STORAGE_CONFIG = 3;
  public static final byte SUBSCRIBE_PUSH_SCHEMA = 4;
  public static final byte SUBSCRIBE_PUSH_INDEX_MANAGER = 5;
  public static final byte SUBSCRIBE_PUSH_FUNCTIONS = 6;
  public static final byte SUBSCRIBE_PUSH_SEQUENCES = 7;

  // Used by the client to identify what data was pushed
  public static final byte REQUEST_PUSH_DISTRIB_CONFIG = 80;
  public static final byte REQUEST_PUSH_LIVE_QUERY = 81; // SINCE 2.1
  public static final byte REQUEST_PUSH_STORAGE_CONFIG = 82;
  public static final byte REQUEST_PUSH_SCHEMA = 83;
  public static final byte REQUEST_PUSH_INDEX_MANAGER = 84;
  public static final byte REQUEST_PUSH_FUNCTIONS = 85;
  public static final byte REQUEST_PUSH_SEQUENCES = 86;

  // Default encoding, in future will be possible to have other encodings
  public static final byte ENCODING_DEFAULT = 0;

  // Error encoding
  public static final byte ERROR_MESSAGE_JAVA = 0;
  public static final byte ERROR_MESSAGE_STRING = 1;
  public static final byte ERROR_MESSAGE_NONE = 1;
}

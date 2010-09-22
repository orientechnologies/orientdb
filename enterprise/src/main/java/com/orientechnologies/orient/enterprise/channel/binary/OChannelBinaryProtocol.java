/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.enterprise.channel.binary;

public class OChannelBinaryProtocol {
  public static final int   CURRENT_VERSION        = 0;

  // COMMANDS
  public static final short CONNECT                = 1;

  public static final byte  DB_OPEN                = 5;
  public static final byte  DB_CREATE              = 6;
  public static final byte  DB_CLOSE               = 7;
  public static final byte  DB_EXIST               = 8;

  public static final byte  DATACLUSTER_ADD        = 10;
  public static final byte  DATACLUSTER_REMOVE     = 11;
  public static final byte  DATACLUSTER_COUNT      = 12;
  public static final byte  DATACLUSTER_DATARANGE  = 13;

  public static final byte  DATASEGMENT_ADD        = 20;
  public static final byte  DATASEGMENT_REMOVE     = 21;

  public static final byte  RECORD_LOAD            = 30;
  public static final byte  RECORD_CREATE          = 31;
  public static final byte  RECORD_UPDATE          = 32;
  public static final byte  RECORD_DELETE          = 33;

  public static final byte  COUNT                  = 40;
  public static final byte  COMMAND                = 41;

  public static final byte  DICTIONARY_LOOKUP      = 50;
  public static final byte  DICTIONARY_PUT         = 51;
  public static final byte  DICTIONARY_REMOVE      = 52;
  public static final byte  DICTIONARY_SIZE        = 53;
  public static final byte  DICTIONARY_KEYS        = 54;

  public static final byte  TX_COMMIT              = 60;

  public static final byte  NODECLUSTER_CONNECT    = 80;
  public static final byte  NODECLUSTER_DISCONNECT = 81;

  // STATUSES
  public static final byte  OK                     = 0;
  public static final byte  ERROR                  = 1;

  // CONSTANTS
  public static final int   RECORD_NULL            = -2;
}

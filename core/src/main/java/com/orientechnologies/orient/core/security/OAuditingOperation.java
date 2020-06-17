/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(-at-)orientdb.com)
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
package com.orientechnologies.orient.core.security;

import com.orientechnologies.orient.core.db.record.ORecordOperation;

/**
 * Enumerates the available auditing OAuditingOperation types.
 *
 * @author S. Colin Leister
 */
public enum OAuditingOperation {
  UNSPECIFIED((byte) -1, "unspecified"),
  CREATED(ORecordOperation.CREATED, "created"),
  LOADED(ORecordOperation.LOADED, "loaded"),
  UPDATED(ORecordOperation.UPDATED, "updated"),
  DELETED(ORecordOperation.DELETED, "deleted"),
  COMMAND((byte) 4, "command"),
  CREATEDCLASS((byte) 5, "createdClass"),
  DROPPEDCLASS((byte) 6, "droppedClass"),
  CHANGEDCONFIG((byte) 7, "changedConfig"),
  NODEJOINED((byte) 8, "nodeJoined"),
  NODELEFT((byte) 9, "nodeLeft"),
  SECURITY((byte) 10, "security"),
  RELOADEDSECURITY((byte) 11, "reloadedSecurity"),
  CHANGED_PWD((byte) 12, "changedPassword");

  private byte byteOp = -1; // -1: unspecified;
  private String stringOp = "unspecified";

  private OAuditingOperation(byte byteOp, String stringOp) {
    this.byteOp = byteOp;
    this.stringOp = stringOp;
  }

  public byte getByte() {
    return byteOp;
  }

  @Override
  public String toString() {
    return stringOp;
  }

  public static OAuditingOperation getByString(String value) {
    if (value == null || value.isEmpty()) return UNSPECIFIED;

    for (OAuditingOperation op : values()) {
      if (op.toString().equalsIgnoreCase(value)) return op;
    }

    return UNSPECIFIED;
  }

  public static OAuditingOperation getByByte(byte value) {
    for (OAuditingOperation op : values()) {
      if (op.getByte() == value) return op;
    }

    return UNSPECIFIED;
  }
}

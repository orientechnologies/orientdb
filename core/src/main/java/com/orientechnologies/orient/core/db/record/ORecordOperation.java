/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Contains the information about a database operation.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ORecordOperation implements Comparable {

  private static final long serialVersionUID = 1L;

  public static final byte  LOADED           = 0;
  public static final byte  UPDATED          = 1;
  public static final byte  DELETED          = 2;
  public static final byte  CREATED          = 3;

  public byte               type;
  public OIdentifiable      record;

  public ORecordOperation() {
  }

  public ORecordOperation(final OIdentifiable iRecord, final byte iStatus) {
    // CLONE RECORD AND CONTENT
    this.record = iRecord;
    this.type = iStatus;
  }

  @Override
  public int hashCode() {
    return record.getIdentity().hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof ORecordOperation))
      return false;

    return record.equals(((ORecordOperation) obj).record);
  }

  @Override
  public String toString() {
    return new StringBuilder(128).append("ORecordOperation [record=").append(record).append(", type=").append(getName(type))
        .append("]").toString();
  }

  public OIdentifiable setRecord(final OIdentifiable record) {
    this.record = record;
    return record;
  }

  public ORecord getRecord() {
    return record != null ? record.getRecord() : null;
  }

  public ORID getRID() {
    return record != null ? record.getIdentity() : null;
  }

  public static String getName(final int type) {
    String operation = "?";
    switch (type) {
    case ORecordOperation.CREATED:
      operation = "CREATE";
      break;
    case ORecordOperation.UPDATED:
      operation = "UPDATE";
      break;
    case ORecordOperation.DELETED:
      operation = "DELETE";
      break;
    case ORecordOperation.LOADED:
      operation = "READ";
      break;
    }
    return operation;
  }

  public static byte getId(String iName) {
    iName = iName.toUpperCase();

    if (iName.startsWith("CREAT"))
      return ORecordOperation.CREATED;
    else if (iName.startsWith("UPDAT"))
      return ORecordOperation.UPDATED;
    else if (iName.startsWith("DELET"))
      return ORecordOperation.DELETED;
    else if (iName.startsWith("READ"))
      return ORecordOperation.LOADED;
    return -1;
  }

  @Override
  public int compareTo(Object o) {
    return record.compareTo(((ORecordOperation) o).record);
  }
}

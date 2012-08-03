/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;

/**
 * Exception thrown when MVCC is enabled and a record cannot be updated or deleted because versions don't match.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OConcurrentModificationException extends ONeedRetryException {

  private static final String MESSAGE_RECORD_VERSION = "your=v";
  private static final String MESSAGE_DB_VERSION     = "db=v";

  private static final long   serialVersionUID       = 1L;

  private ORID                rid;
  private int                 databaseVersion;
  private int                 recordVersion;

  /**
   * Default constructor for OFastConcurrentModificationException
   */
  protected OConcurrentModificationException() {
    rid = new ORecordId();
    databaseVersion = 0;
    recordVersion = 0;
  }

  public OConcurrentModificationException(final String message) {
    int beginPos = message.indexOf(ORID.PREFIX);
    int endPos = message.indexOf(' ', beginPos);
    rid = new ORecordId(message.substring(beginPos, endPos));

    beginPos = message.indexOf(MESSAGE_DB_VERSION, endPos) + MESSAGE_DB_VERSION.length();
    endPos = message.indexOf(' ', beginPos);
    databaseVersion = Integer.parseInt(message.substring(beginPos, endPos));

    beginPos = message.indexOf(MESSAGE_RECORD_VERSION, endPos) + MESSAGE_RECORD_VERSION.length();
    endPos = message.indexOf(')', beginPos);
    recordVersion = Integer.parseInt(message.substring(beginPos, endPos));
  }

  public OConcurrentModificationException(final ORID iRID, final int iDatabaseVersion, final int iRecordVersion) {
    if (OFastConcurrentModificationException.enabled())
      throw new IllegalStateException("Fast-throw is enabled. Use OFastConcurrentModificationException.instance() instead");

    rid = iRID;
    databaseVersion = iDatabaseVersion;
    recordVersion = iRecordVersion;
  }

  public int getDatabaseVersion() {
    return databaseVersion;
  }

  public int getRecordVersion() {
    return recordVersion;
  }

  public ORID getRid() {
    return rid;
  }

  public String getMessage() {
    StringBuilder sb = new StringBuilder();
    sb.append("Cannot delete the record ");
    sb.append(rid);
    sb.append(" because the version is not the latest. Probably you are deleting an old record or it has been modified by another user (db=v");
    sb.append(databaseVersion);
    sb.append(" your=v");
    sb.append(recordVersion);
    sb.append(")");
    return sb.toString();
  }
}

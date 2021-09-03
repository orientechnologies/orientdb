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
package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.exception.OErrorCode;
import com.orientechnologies.common.exception.OHighLevelException;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.id.ORID;
import java.util.Locale;
import java.util.Objects;

/**
 * Exception thrown when MVCC is enabled and a record cannot be updated or deleted because versions
 * don't match.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OConcurrentModificationException extends ONeedRetryException
    implements OHighLevelException {

  private static final long serialVersionUID = 1L;

  private ORID rid;
  private int databaseVersion = 0;
  private int recordVersion = 0;
  private int recordOperation;

  public OConcurrentModificationException(OConcurrentModificationException exception) {
    super(exception, OErrorCode.MVCC_ERROR);

    this.rid = exception.rid;
    this.recordVersion = exception.recordVersion;
    this.databaseVersion = exception.databaseVersion;
    this.recordOperation = exception.recordOperation;
  }

  protected OConcurrentModificationException(final String message) {
    super(message);
  }

  public OConcurrentModificationException(
      final ORID iRID,
      final int iDatabaseVersion,
      final int iRecordVersion,
      final int iRecordOperation) {
    super(
        makeMessage(iRecordOperation, iRID, iDatabaseVersion, iRecordVersion),
        OErrorCode.MVCC_ERROR);

    if (OFastConcurrentModificationException.enabled())
      throw new IllegalStateException(
          "Fast-throw is enabled. Use OFastConcurrentModificationException.instance() instead");

    rid = iRID;
    databaseVersion = iDatabaseVersion;
    recordVersion = iRecordVersion;
    recordOperation = iRecordOperation;
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof OConcurrentModificationException)) return false;

    final OConcurrentModificationException other = (OConcurrentModificationException) obj;

    if (recordOperation == other.recordOperation && rid.equals(other.rid)) {
      if (databaseVersion == other.databaseVersion) return recordOperation == other.recordOperation;
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(rid, databaseVersion, recordVersion, recordOperation);
  }

  public int getEnhancedDatabaseVersion() {
    return databaseVersion;
  }

  public int getEnhancedRecordVersion() {
    return recordVersion;
  }

  public ORID getRid() {
    return rid;
  }

  private static String makeMessage(
      int recordOperation, ORID rid, int databaseVersion, int recordVersion) {
    final String operation = ORecordOperation.getName(recordOperation);

    final StringBuilder sb = new StringBuilder();
    sb.append("Cannot ");
    sb.append(operation);
    sb.append(" the record ");
    sb.append(rid);
    sb.append(" because the version is not the latest. Probably you are ");
    sb.append(operation.toLowerCase(Locale.ENGLISH).substring(0, operation.length() - 1));
    sb.append("ing an old record or it has been modified by another user (db=v");
    sb.append(databaseVersion);
    sb.append(" your=v");
    sb.append(recordVersion);
    sb.append(")");
    return sb.toString();
  }
}

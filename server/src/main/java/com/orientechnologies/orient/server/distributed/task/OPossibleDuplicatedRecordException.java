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
package com.orientechnologies.orient.server.distributed.task;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;

/**
 * Exception thrown when a record is duplicated by a distributed transaction that is not committed. This means that if the
 * transaction is committed (2PC) the record would be duplicated, otherwise the record could be inserted with another retry.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OPossibleDuplicatedRecordException extends ONeedRetryException {
  private final ORID   rid;
  private final String indexName;

  public OPossibleDuplicatedRecordException(final OPossibleDuplicatedRecordException exception) {
    super(exception);
    this.indexName = exception.indexName;
    this.rid = exception.rid;
  }

  public OPossibleDuplicatedRecordException(final ORecordDuplicatedException exception) {
    super(exception.getMessage());
    this.indexName = exception.getIndexName();
    this.rid = exception.getRid();
  }

  public ORID getRid() {
    return rid;
  }

  public String getIndexName() {
    return indexName;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null || !obj.getClass().equals(getClass()))
      return false;

    if (!indexName.equals(((OPossibleDuplicatedRecordException) obj).indexName))
      return false;

    return rid.equals(((OPossibleDuplicatedRecordException) obj).rid);
  }

  @Override
  public int hashCode() {
    return rid.hashCode();
  }

  @Override
  public String toString() {
    return super.toString() + " INDEX=" + indexName + " RID=" + rid;
  }
}

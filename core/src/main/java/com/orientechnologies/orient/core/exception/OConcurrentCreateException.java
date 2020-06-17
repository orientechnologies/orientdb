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
import com.orientechnologies.common.exception.OHighLevelException;
import com.orientechnologies.orient.core.id.ORID;
import java.util.Objects;

/**
 * Exception thrown when a create operation get a non expected RID. This could happen with
 * distributed inserts. The client should retry to re-execute the operation.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OConcurrentCreateException extends ONeedRetryException implements OHighLevelException {

  private static final long serialVersionUID = 1L;

  private ORID expectedRid;
  private ORID actualRid;

  public OConcurrentCreateException(OConcurrentCreateException exception) {
    super(exception, null);

    this.expectedRid = exception.expectedRid;
    this.actualRid = exception.actualRid;
  }

  protected OConcurrentCreateException(final String message) {
    super(message);
  }

  public OConcurrentCreateException(final ORID expectedRID, final ORID actualRid) {
    super(makeMessage(expectedRID, actualRid));

    this.expectedRid = expectedRID;
    this.actualRid = actualRid;
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof OConcurrentCreateException)) return false;

    final OConcurrentCreateException other = (OConcurrentCreateException) obj;

    return expectedRid.equals(other.expectedRid) && actualRid.equals(other.actualRid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expectedRid, actualRid);
  }

  public ORID getExpectedRid() {
    return expectedRid;
  }

  public ORID getActualRid() {
    return actualRid;
  }

  private static String makeMessage(ORID expectedRid, ORID actualRid) {
    final StringBuilder sb = new StringBuilder();
    sb.append("Cannot create the record ");
    sb.append(expectedRid);
    sb.append(" because the assigned RID was ");
    sb.append(actualRid);
    sb.append(" instead");
    return sb.toString();
  }
}

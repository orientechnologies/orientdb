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

import com.orientechnologies.common.exception.OHighLevelException;
import com.orientechnologies.orient.core.id.ORID;
import java.util.Objects;

public class ORecordNotFoundException extends OCoreException implements OHighLevelException {

  private static final long serialVersionUID = -265573123216968L;

  private ORID rid;

  public ORecordNotFoundException(final ORecordNotFoundException exception) {
    super(exception);
    this.rid = exception.rid;
  }

  public ORecordNotFoundException(final ORID iRID) {
    super("The record with id '" + iRID + "' was not found");
    rid = iRID;
  }

  public ORecordNotFoundException(final ORID iRID, final String message) {
    super(message);
    rid = iRID;
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof ORecordNotFoundException)) return false;

    if (rid == null && ((ORecordNotFoundException) obj).rid == null)
      return toString().equals(obj.toString());

    return rid != null
        ? rid.equals(((ORecordNotFoundException) obj).rid)
        : ((ORecordNotFoundException) obj).rid.equals(rid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(rid);
  }

  public ORID getRid() {
    return rid;
  }
}

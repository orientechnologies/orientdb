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

package com.orientechnologies.orient.core.storage;

import com.orientechnologies.common.exception.OHighLevelException;
import com.orientechnologies.orient.core.exception.OCoreException;
import com.orientechnologies.orient.core.id.ORID;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 * @since 9/5/12
 */
public class ORecordDuplicatedException extends OCoreException implements OHighLevelException {
  private final ORID rid;

  public ORecordDuplicatedException(ORecordDuplicatedException exception) {
    super(exception);
    this.rid = exception.rid;
  }

  public ORecordDuplicatedException(final String message, final ORID iRid) {
    super(message);
    this.rid = iRid;
  }

  public ORID getRid() {
    return rid;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null || !obj.getClass().equals(getClass()))
      return false;

    return rid.equals(((ORecordDuplicatedException) obj).rid);
  }

  @Override
  public int hashCode() {
    return rid.hashCode();
  }

  @Override
  public String toString() {
    return super.toString() + " RID=" + rid;
  }
}

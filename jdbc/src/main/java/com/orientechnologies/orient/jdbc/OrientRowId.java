/**
 * Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For more information: http://www.orientechnologies.com
 */
package com.orientechnologies.orient.jdbc;

import java.sql.RowId;

import com.orientechnologies.orient.core.id.ORID;

public class OrientRowId implements RowId {

  protected final ORID rid;

  public OrientRowId(final ORID rid) {
    this.rid = rid;
  }

  @Override
  public byte[] getBytes() {
    return rid.toStream();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof OrientRowId)
      return rid.equals(((OrientRowId) obj).rid);
    return false;
  }

  @Override
  public int hashCode() {
    return rid.hashCode();
  }

  @Override
  public String toString() {
    return rid.toString();
  }
}

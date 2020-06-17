/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
package com.orientechnologies.orient.object.enumerations;

import com.orientechnologies.orient.core.record.ORecord;
import java.io.Serializable;
import java.util.Iterator;

/**
 * Lazy implementation of Iterator that load the records only when accessed. It keep also track of
 * changes to the source record avoiding to call setDirty() by hand.
 *
 * @author Luca Molino (molino.luca--at--gmail.com)
 */
@SuppressWarnings({"unchecked"})
public class OObjectEnumLazyIterator<TYPE extends Enum> implements Iterator<TYPE>, Serializable {
  private static final long serialVersionUID = -4012483076050044405L;

  private final ORecord sourceRecord;
  private final Iterator<? extends Object> underlying;
  private final Class<Enum> enumClass;

  public OObjectEnumLazyIterator(
      final Class<Enum> iEnumClass,
      final ORecord iSourceRecord,
      final Iterator<? extends Object> iIterator) {
    this.sourceRecord = iSourceRecord;
    this.underlying = iIterator;
    this.enumClass = iEnumClass;
  }

  public TYPE next() {
    final Object value = underlying.next();
    if (value instanceof Number)
      return (TYPE) enumClass.getEnumConstants()[((Number) value).intValue()];
    else return (TYPE) Enum.valueOf(enumClass, value.toString());
  }

  public boolean hasNext() {
    return underlying.hasNext();
  }

  public void remove() {
    underlying.remove();
    if (sourceRecord != null) sourceRecord.setDirty();
  }
}

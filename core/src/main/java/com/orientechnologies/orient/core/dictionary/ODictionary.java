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
package com.orientechnologies.orient.core.dictionary;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import java.util.Optional;
import java.util.stream.Stream;

@SuppressWarnings({"unchecked", "DeprecatedIsStillUsed"})
@Deprecated
public class ODictionary<T> {
  private final OIndex index;

  public ODictionary(final OIndex iIndex) {
    index = iIndex;
  }

  public <RET extends T> RET get(final String iKey) {
    final Optional<ORID> value;
    try (final Stream<ORID> stream = index.getInternal().getRids(iKey)) {
      value = stream.findAny();
    }
    return value.map(rid -> (RET) rid.getRecord()).orElse(null);
  }

  public <RET extends T> RET get(final String iKey, final String fetchPlan) {
    final Optional<ORID> value;
    try (Stream<ORID> stream = index.getInternal().getRids(iKey)) {
      value = stream.findAny();
    }
    return value
        .map(rid -> (RET) ODatabaseRecordThreadLocal.instance().get().load(rid, fetchPlan))
        .orElse(null);
  }

  public void put(final String iKey, final Object iValue) {
    index.put(iKey, (OIdentifiable) iValue);
  }

  public boolean remove(final String iKey) {
    return index.remove(iKey);
  }

  public long size() {
    return index.getInternal().size();
  }

  public OIndex getIndex() {
    return index;
  }
}

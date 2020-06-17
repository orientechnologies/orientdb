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
package com.orientechnologies.orient.object.dictionary;

import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

/** Wrapper of dictionary instance that convert values in records. */
@SuppressWarnings("DeprecatedIsStillUsed")
@Deprecated
public class ODictionaryWrapper extends ODictionary<Object> {
  private final ODatabaseObject database;

  public ODictionaryWrapper(final ODatabaseObject iDatabase, OIndex index) {
    super(index);
    this.database = iDatabase;
  }

  @SuppressWarnings("unchecked")
  public <RET> RET get(final String iKey, final String fetchPlan) {
    final ORecord record = super.get(iKey);
    return (RET) database.getUserObjectByRecord(record, fetchPlan);
  }

  public void put(final String iKey, final Object iValue) {
    final ODocument record = (ODocument) database.getRecordByUserObject(iValue, false);
    super.put(iKey, record);
  }
}

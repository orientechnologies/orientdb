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
package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.ODatabaseException;

public class ODatabaseRecordThreadLocal extends ThreadLocal<ODatabaseRecord> {

  public static ODatabaseRecordThreadLocal INSTANCE = new ODatabaseRecordThreadLocal();

  @Override
  public ODatabaseRecord get() {
    ODatabaseRecord db = super.get();
    if (db == null) {
      if (Orient.instance().getDatabaseThreadFactory() == null) {
        throw new ODatabaseException(
            "Database instance is not set in current thread. Assure to set it with: ODatabaseRecordThreadLocal.INSTANCE.set(db);");
      } else {
        db = Orient.instance().getDatabaseThreadFactory().getThreadDatabase();
        if (db == null) {
          throw new ODatabaseException(
              "Database instance is not set in current thread. Assure to set it with: ODatabaseRecordThreadLocal.INSTANCE.set(db);");
        } else {
          set(db);
        }
      }
    }
    return db;
  }

  @Override
  public void remove() {
    super.remove();
  }
  
  @Override
  public void set(final ODatabaseRecord value) {
    super.set(value);
  }

  public ODatabaseRecord getIfDefined() {
    return super.get();
  }

  public boolean isDefined() {
    return super.get() != null;
  }

}

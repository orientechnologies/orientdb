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
package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import java.util.concurrent.atomic.AtomicReference;

public class ODatabaseRecordThreadLocal extends ThreadLocal<ODatabaseDocumentInternal> {

  private static final AtomicReference<ODatabaseRecordThreadLocal> INSTANCE =
      new AtomicReference<>();

  public static ODatabaseRecordThreadLocal instance() {
    final ODatabaseRecordThreadLocal dbInst = INSTANCE.get();

    if (dbInst != null) return dbInst;

    // we can do that to avoid thread local memory leaks in containers
    if (INSTANCE.get() == null) {
      final Orient inst = Orient.instance();
      inst.registerListener(
          new OOrientListenerAbstract() {
            @Override
            public void onStartup() {}

            @Override
            public void onShutdown() {
              INSTANCE.set(null);
            }
          });

      INSTANCE.compareAndSet(null, new ODatabaseRecordThreadLocal());
    }
    return INSTANCE.get();
  }

  @Override
  public ODatabaseDocumentInternal get() {
    ODatabaseDocumentInternal db = super.get();
    if (db == null) {
      if (Orient.instance().getDatabaseThreadFactory() == null) {
        throw new ODatabaseException(
            "The database instance is not set in the current thread. Be sure to set it with: ODatabaseRecordThreadLocal.instance().set(db);");
      } else {
        db = Orient.instance().getDatabaseThreadFactory().getThreadDatabase();
        if (db == null) {
          throw new ODatabaseException(
              "The database instance is not set in the current thread. Be sure to set it with: ODatabaseRecordThreadLocal.instance().set(db);");
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
  public void set(final ODatabaseDocumentInternal value) {
    super.set(value);
  }

  public ODatabaseDocumentInternal getIfDefined() {
    return super.get();
  }

  public boolean isDefined() {
    return super.get() != null;
  }
}

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
package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.exception.ODatabaseException;

public class ODatabaseRecordThreadLocal extends ThreadLocal<ODatabaseDocumentInternal> {

  public static volatile ODatabaseRecordThreadLocal INSTANCE = new ODatabaseRecordThreadLocal();

  static {
    final Orient inst = Orient.instance();
    inst.registerListener(new OOrientListenerAbstract() {
      @Override
      public void onStartup() {
        if (INSTANCE == null)
          INSTANCE = new ODatabaseRecordThreadLocal();
      }

      @Override
      public void onShutdown() {
        INSTANCE = null;
      }
    });
  }

  @Override
  public ODatabaseDocumentInternal get() {
    ODatabaseDocumentInternal db = super.get();
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

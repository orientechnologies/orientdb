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

import java.util.concurrent.atomic.AtomicReference;

public class ODatabaseRecordThreadLocal extends ThreadLocal<ODatabaseDocumentInternal> {
  /**
   * @deprecated will be removed in 3.0 version, use {{@link #instance()}} instead.
   */
  @Deprecated
  public static final InstanceHolder INSTANCE = new InstanceHolder();

  private static final AtomicReference<ODatabaseRecordThreadLocal> INST_HOLDER = new AtomicReference<ODatabaseRecordThreadLocal>();

  public static ODatabaseRecordThreadLocal instance() {
    final ODatabaseRecordThreadLocal dbInst = INST_HOLDER.get();

    if (dbInst != null)
      return dbInst;

    //we can do that to avoid thread local memory leaks in containers
    if (INST_HOLDER.get() == null) {
      final Orient inst = Orient.instance();
      inst.registerListener(new OOrientListenerAbstract() {
        @Override
        public void onStartup() {
        }

        @Override
        public void onShutdown() {
          INST_HOLDER.set(null);
        }
      });

      INST_HOLDER.compareAndSet(null, new ODatabaseRecordThreadLocal());

    }
    return INST_HOLDER.get();
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

  public static final class InstanceHolder {
    public ODatabaseDocumentInternal get() {
      return ODatabaseRecordThreadLocal.instance().get();
    }

    public void remove() {
      ODatabaseRecordThreadLocal.instance().remove();
    }

    public void set(final ODatabaseDocumentInternal value) {
      ODatabaseRecordThreadLocal.instance().set(value);
    }

    public ODatabaseDocumentInternal getIfDefined() {
      return ODatabaseRecordThreadLocal.instance().getIfDefined();
    }

    public boolean isDefined() {
      return ODatabaseRecordThreadLocal.instance().isDefined();
    }
  }
}

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

package com.orientechnologies.orient.core.cache;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.Map;
import java.util.TimerTask;
import java.util.WeakHashMap;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class OUnboundedWeakCache extends OAbstractMapCache<WeakHashMap<ORID, WeakReference<ORecord>>> implements OCache {
  public static final int CLEAN_WEAK_ENTRIES_DELAY = 10000;
  private Object          lock                     = new Object();

  public OUnboundedWeakCache() {
    super(new WeakHashMap<ORID, WeakReference<ORecord>>());
    Orient.instance().getTimer().schedule(new TimerTask() {
      @Override
      public void run() {
        synchronized (lock) {
          final Iterator<Map.Entry<ORID, WeakReference<ORecord>>> it = cache.entrySet().iterator();
          while (it.hasNext()) {
            if (it.next().getValue().get() == null)
              it.remove();
          }
        }
      }
    }, CLEAN_WEAK_ENTRIES_DELAY, CLEAN_WEAK_ENTRIES_DELAY);
  }

  @Override
  public ORecord get(final ORID rid) {
    final WeakReference<ORecord> value;
    synchronized (lock) {
      value = cache.get(rid);
    }
    return get(value);
  }

  @Override
  public ORecord put(final ORecord record) {
    final WeakReference<ORecord> value;
    synchronized (lock) {
      value = cache.put(record.getIdentity(), new WeakReference<ORecord>(record));
    }
    return get(value);
  }

  @Override
  public ORecord remove(final ORID rid) {
    final WeakReference<ORecord> value;
    synchronized (lock) {
      value = cache.remove(rid);
    }
    return get(value);
  }

  private ORecord get(WeakReference<ORecord> value) {
    if (value == null)
      return null;
    else
      return value.get();
  }
}

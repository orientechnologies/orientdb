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

import java.lang.ref.WeakReference;
import java.util.WeakHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class OUnboundedWeakCache extends OAbstractMapCache<WeakHashMap<ORID, WeakReference<ORecord>>> implements OCache {

  private final Lock lock = new ReentrantLock();

  public OUnboundedWeakCache() {
    super(new WeakHashMap<ORID, WeakReference<ORecord>>());
  }

  @Override
  public ORecord get(final ORID rid) {
    final WeakReference<ORecord> value;
    lock.lock();
    try {
      value = cache.get(rid);
    } finally {
      lock.unlock();
    }
    return get(value);
  }

  @Override
  public ORecord put(final ORecord record) {
    final WeakReference<ORecord> value;
    lock.lock();
    try {
      value = cache.put(record.getIdentity(), new WeakReference<ORecord>(record));
    } finally {
      lock.unlock();
    }
    return get(value);
  }

  @Override
  public ORecord remove(final ORID rid) {
    final WeakReference<ORecord> value;
    lock.lock();
    try {
      value = cache.remove(rid);
    } finally {
      lock.unlock();
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

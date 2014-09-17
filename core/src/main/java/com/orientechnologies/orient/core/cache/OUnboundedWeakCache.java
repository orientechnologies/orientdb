/*
 * Copyright 2010-2012 Luca Garulli (l.garulli(at)orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.cache;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;

import java.lang.ref.WeakReference;
import java.util.WeakHashMap;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OUnboundedWeakCache extends OAbstractMapCache<WeakHashMap<ORID, WeakReference<ORecordInternal>>> implements OCache {

  public OUnboundedWeakCache() {
    super(new WeakHashMap<ORID, WeakReference<ORecordInternal>>());
  }

  @Override
  public int limit() {
    return Integer.MAX_VALUE;
  }

  @Override
  public ORecordInternal get(final ORID id) {
    if (!isEnabled())
      return null;

    return get(cache.get(id));
  }

  @Override
  public ORecordInternal put(final ORecordInternal record) {
    if (!isEnabled())
      return null;

    return get(cache.put(record.getIdentity(), new WeakReference<ORecordInternal>(record)));
  }

  @Override
  public ORecordInternal remove(final ORID id) {
    if (!isEnabled())
      return null;

    return get(cache.remove(id));
  }

  private ORecordInternal get(WeakReference<ORecordInternal> value) {
    if (value == null)
      return null;
    else
      return value.get();
  }
}

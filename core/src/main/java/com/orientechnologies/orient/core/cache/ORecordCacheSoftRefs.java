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

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;

/**
 * Cache implementation that uses Soft References.
 * 
 * @author Luca Garulli (l.garulli-at-orientdb.com)
 */
public class ORecordCacheSoftRefs extends OAbstractMapCache<OSoftRefsHashMap<ORID, ORecord>> implements ORecordCache {

  public ORecordCacheSoftRefs() {
    super(new OSoftRefsHashMap<ORID, ORecord>());
  }

  @Override
  public ORecord get(final ORID rid) {
    if (!isEnabled())
      return null;

    return cache.get(rid);
  }

  @Override
  public ORecord put(final ORecord record) {
    if (!isEnabled())
      return null;
    return cache.put(record.getIdentity(), record);
  }

  @Override
  public ORecord remove(final ORID rid) {
    if (!isEnabled())
      return null;
    return cache.remove(rid);
  }

  @Override
  public void shutdown() {
    clear();
  }

  @Override
  public void clear() {
    cache.clear();
    cache = new OSoftRefsHashMap<ORID, ORecord>();
  }
}

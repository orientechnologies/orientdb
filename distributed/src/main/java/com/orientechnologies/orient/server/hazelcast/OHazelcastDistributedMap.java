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
package com.orientechnologies.orient.server.hazelcast;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.map.listener.MapClearedListener;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Optimized concurrent hash map implementation on top of Hazelcast distributed map.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OHazelcastDistributedMap extends ConcurrentHashMap<String, Object> implements EntryAddedListener<String, Object>,
    EntryRemovedListener<String, Object>, MapClearedListener, EntryUpdatedListener<String, Object> {
  private final IMap<String, Object> hzMap;
  private final String               membershipListenerRegistration;

  public OHazelcastDistributedMap(final HazelcastInstance hz) {
    hzMap = hz.getMap("orientdb");
    membershipListenerRegistration = hzMap.addEntryListener(this, true);

    super.putAll(hzMap);
  }

  public IMap<String, Object> getHazelcastMap() {
    return hzMap;
  }

  @Override
  public Object get(final Object key) {
    return hzMap.get(key);
  }

  public Object getLocalCachedValue(final Object key) {
    final Object res = super.get(key);
    if (res != null)
      return res;

    return hzMap.get(key);
  }

  @Override
  public Object put(final String key, final Object value) {
    hzMap.put(key, value);
    return super.put(key, value);
  }

  public Object putInLocalCache(final String key, final Object value) {
    return super.put(key, value);
  }

  @Override
  public Object remove(final Object key) {
    hzMap.remove(key);
    return super.remove(key);
  }

  @Override
  public boolean remove(final Object key, final Object value) {
    hzMap.remove(key, value);
    return super.remove(key, value);
  }

  @Override
  public void entryAdded(EntryEvent<String, Object> event) {
    super.put(event.getKey(), event.getValue());
  }

  @Override
  public void entryRemoved(EntryEvent<String, Object> event) {
    super.remove(event.getKey());
  }

  @Override
  public void entryUpdated(EntryEvent<String, Object> event) {
    super.put(event.getKey(), event.getValue());
  }

  @Override
  public void mapCleared(MapEvent event) {
    super.clear();
  }

  public void destroy() {
    clear();
    hzMap.removeEntryListener(membershipListenerRegistration);
  }
}

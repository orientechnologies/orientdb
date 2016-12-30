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

import com.hazelcast.core.*;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.map.listener.MapClearedListener;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Optimized concurrent hash map implementation on top of Hazelcast distributed map.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OHazelcastDistributedMap extends ConcurrentHashMap<String, Object> implements EntryAddedListener<String, Object>,
    EntryRemovedListener<String, Object>, MapClearedListener, EntryUpdatedListener<String, Object> {
  private final OHazelcastPlugin     dManager;
  private final IMap<String, Object> hzMap;
  private final String               membershipListenerRegistration;

  public static final String         ORIENTDB_MAP = "orientdb";

  public OHazelcastDistributedMap(final OHazelcastPlugin manager, final HazelcastInstance hz) {
    dManager = manager;
    hzMap = hz.getMap(ORIENTDB_MAP);
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

  @Override
  public boolean containsKey(final Object key) {
    return hzMap.containsKey(key);
  }

  @Override
  public Set<Entry<String, Object>> entrySet() {
    return hzMap.entrySet();
  }

  public Set<Entry<String, Object>> localEntrySet() {
    return super.entrySet();
  }

  public Object getLocalCachedValue(final Object key) {
    final Object res = super.get(key);
    if (res != null)
      return res;

    try {
      return hzMap.get(key);
    } catch (HazelcastInstanceNotActiveException e) {
      // IGNORE IT
      return null;
    }
  }

  @Override
  public Object put(final String key, final Object value) {
    try {
      hzMap.put(key, value);
    } catch (HazelcastInstanceNotActiveException e) {
      // IGNORE IT
    }
    return super.put(key, value);
  }

  @Override
  public Object putIfAbsent(final String key, final Object value) {
    try {
      hzMap.putIfAbsent(key, value);
    } catch (HazelcastInstanceNotActiveException e) {
      // IGNORE IT
    }
    return super.putIfAbsent(key, value);
  }

  public Object putInLocalCache(final String key, final Object value) {
    return super.put(key, value);
  }

  @Override
  public Object remove(final Object key) {
    try {
      hzMap.remove(key);
    } catch (HazelcastInstanceNotActiveException e) {
      // IGNORE IT
    }
    return super.remove(key);
  }

  @Override
  public boolean remove(final Object key, final Object value) {
    try {
      hzMap.remove(key, value);
    } catch (HazelcastInstanceNotActiveException e) {
      // IGNORE IT
    }
    return super.remove(key, value);
  }

  @Override
  public void entryAdded(final EntryEvent<String, Object> event) {
    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
          "Map entry added key=" + event.getKey() + ": " + event.getValue());
    super.put(event.getKey(), event.getValue());
  }

  @Override
  public void entryUpdated(final EntryEvent<String, Object> event) {
    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
          "Map entry updated key=" + event.getKey() + ": " + event.getValue());

    super.put(event.getKey(), event.getValue());
  }

  @Override
  public void entryRemoved(final EntryEvent<String, Object> event) {
    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, dManager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
          "Map entry removed key=" + event.getKey() + ": " + event.getValue());
    super.remove(event.getKey());
  }

  @Override
  public void mapCleared(MapEvent event) {
    super.clear();
  }

  public void destroy() {
    clear();
    hzMap.removeEntryListener(membershipListenerRegistration);
  }

  public void clearLocalCache() {
    super.clear();
  }
}

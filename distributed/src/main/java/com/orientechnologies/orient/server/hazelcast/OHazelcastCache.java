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
package com.orientechnologies.orient.server.hazelcast;

import java.util.Collection;
import java.util.Collections;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.orientechnologies.orient.core.cache.OCache;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerLifecycleListener;

/**
 * 2-Level cache based on the Hazelcast's distributed map
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHazelcastCache implements OCache, OServerLifecycleListener {
  private boolean                        enabled = true;
  private final int                      limit;
  private final String                   mapName;
  private IMap<ORID, ORecordInternal<?>> map;
  private HazelcastInstance              hInstance;
  private OServer                        server;

  public OHazelcastCache(final OServer iServer, final HazelcastInstance iInstance, final String iStorageName, final int iLimit) {
    mapName = iStorageName + ".level2cache";
    limit = iLimit;
    hInstance = iInstance;
    server = iServer;
    server.registerLifecycleListener(this);
  }

  @Override
  public void startup() {
    if (map == null && hInstance != null)
      map = hInstance.getMap(mapName);
  }

  @Override
  public void shutdown() {
    if (map != null && hInstance.getCluster().getMembers().size() <= 1)
      // I'M LAST MEMBER: REMOVE ALL THE ENTRIES
      map.clear();
    map = null;
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public boolean enable() {
    if (!enabled) {
      enabled = true;
      startup();
    }
    return true;
  }

  @Override
  public boolean disable() {
    if (enabled) {
      enabled = false;
      shutdown();
    }
    return true;
  }

  @Override
  public ORecordInternal<?> get(final ORID id) {
    if (map == null)
      return null;
    return map.get(id);
  }

  @Override
  public ORecordInternal<?> put(final ORecordInternal<?> record) {
    if (map == null)
      return null;
    if (limit < 0 || map.size() < limit)
      return map.put(record.getIdentity(), record);
    return null;
  }

  @Override
  public ORecordInternal<?> remove(final ORID id) {
    if (map == null)
      return null;
    return map.remove(id);
  }

  @Override
  public void clear() {
    if (enabled)
      map.clear();
  }

  @Override
  public int size() {
    if (!enabled)
      return 0;
    return map.size();
  }

  @Override
  public int limit() {
    if (!enabled)
      return 0;
    return limit;
  }

  @Override
  public Collection<ORID> keys() {
    if (!enabled)
      return Collections.emptyList();
    return map.keySet();
  }

  /**
   * Hazelcast manages locking automatically.
   */
  @Override
  public void lock(ORID id) {
  }

  /**
   * Hazelcast manages locking automatically.
   */
  @Override
  public void unlock(ORID id) {
  }

  @Override
  public void onBeforeActivate() {
  }

  @Override
  public void onAfterActivate() {
    startup();
  }

  @Override
  public void onBeforeDeactivate() {
    shutdown();
  }

  @Override
  public void onAfterDeactivate() {
  }
}

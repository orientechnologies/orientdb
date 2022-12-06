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
package com.orientechnologies.orient.server.hazelcast;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.map.listener.MapClearedListener;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Optimized concurrent hash map implementation on top of Hazelcast distributed map.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OHazelcastDistributedMap extends ConcurrentHashMap<String, Object>
    implements EntryAddedListener<String, Object>,
        EntryRemovedListener<String, Object>,
        MapClearedListener,
        EntryUpdatedListener<String, Object> {
  private final OHazelcastClusterMetadataManager dManager;
  private final IMap<String, Object> hzMap;
  private final String membershipListenerRegistration;

  public static final String ORIENTDB_MAP = "orientdb";

  public OHazelcastDistributedMap(
      final OHazelcastClusterMetadataManager manager, final HazelcastInstance hz) {
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
    if (res != null) return res;

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
      ODistributedServerLog.debug(
          this,
          dManager.getLocalNodeName(),
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Map entry added "
              + event.getKey()
              + "="
              + event.getValue()
              + " from server "
              + dManager.getNodeName(event.getMember(), true));
    super.put(event.getKey(), event.getValue());
  }

  @Override
  public void entryUpdated(final EntryEvent<String, Object> event) {
    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(
          this,
          dManager.getLocalNodeName(),
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Map entry updated "
              + event.getKey()
              + "="
              + event.getValue()
              + " from server "
              + dManager.getNodeName(event.getMember(), true));

    super.put(event.getKey(), event.getValue());
  }

  @Override
  public void entryRemoved(final EntryEvent<String, Object> event) {
    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(
          this,
          dManager.getLocalNodeName(),
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Map entry removed "
              + event.getKey()
              + "="
              + event.getValue()
              + " from "
              + dManager.getNodeName(event.getMember(), true));
    super.remove(event.getKey());
  }

  @Override
  public void mapCleared(MapEvent event) {
    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(
          this,
          dManager.getLocalNodeName(),
          null,
          ODistributedServerLog.DIRECTION.NONE,
          "Map cleared from server " + dManager.getNodeName(event.getMember(), true));
    super.clear();
  }

  public void destroy() {
    clear();
    hzMap.removeEntryListener(membershipListenerRegistration);
  }

  public void clearLocalCache() {
    super.clear();
  }

  public boolean existsNode(String nodeUuid) {
    return this.containsKey(OHazelcastClusterMetadataManager.CONFIG_NODE_PREFIX + nodeUuid);
  }

  public ODocument getNodeConfig(String nodeUuid) {
    return (ODocument) get(OHazelcastClusterMetadataManager.CONFIG_NODE_PREFIX + nodeUuid);
  }

  public void removeNode(String nodeUuid) {
    this.remove(OHazelcastClusterMetadataManager.CONFIG_NODE_PREFIX + nodeUuid);
  }

  public ODocument getLocalCachedNodeConfig(String nodeUuid) {
    return (ODocument)
        getLocalCachedValue(OHazelcastClusterMetadataManager.CONFIG_NODE_PREFIX + nodeUuid);
  }

  public List<String> getNodes() {
    final List<String> nodes = new ArrayList<String>();

    for (Map.Entry entry : this.entrySet()) {
      if (entry.getKey().toString().startsWith(OHazelcastClusterMetadataManager.CONFIG_NODE_PREFIX))
        nodes.add(
            entry
                .getKey()
                .toString()
                .substring(OHazelcastClusterMetadataManager.CONFIG_NODE_PREFIX.length()));
    }
    return nodes;
  }

  public void putNodeConfig(String nodeUuid, ODocument cfg) {
    put(OHazelcastClusterMetadataManager.CONFIG_NODE_PREFIX + nodeUuid, cfg);
  }

  public Set<String> getNodeUuidByName(String name) {
    Set<String> uuids = new HashSet<String>();
    for (Iterator<Map.Entry<String, Object>> it = this.localEntrySet().iterator(); it.hasNext(); ) {
      final Map.Entry<String, Object> entry = it.next();
      if (entry.getKey().startsWith(OHazelcastClusterMetadataManager.CONFIG_NODE_PREFIX)) {
        final ODocument nodeCfg = (ODocument) entry.getValue();
        if (name.equals(nodeCfg.field("name"))) {
          // FOUND: USE THIS
          final String uuid =
              entry
                  .getKey()
                  .substring(OHazelcastClusterMetadataManager.CONFIG_NODE_PREFIX.length());
          uuids.add(uuid);
        }
      }
    }
    return uuids;
  }

  public static boolean isNodeConfigKey(String key) {
    return key.startsWith(OHazelcastClusterMetadataManager.CONFIG_NODE_PREFIX);
  }

  public ODocument getRegisteredNodes() {
    final ODocument registeredNodes = new ODocument();
    String jsonData = (String) this.get(OHazelcastClusterMetadataManager.CONFIG_REGISTEREDNODES);
    if (jsonData != null) {
      registeredNodes.fromJSON(jsonData);
    }
    return registeredNodes;
  }

  public void putRegisteredNodes(ODocument registeredNodes) {
    this.put(OHazelcastClusterMetadataManager.CONFIG_REGISTEREDNODES, registeredNodes.toJSON());
  }

  public static boolean isRegisteredNodes(String key) {
    return key.startsWith(OHazelcastClusterMetadataManager.CONFIG_REGISTEREDNODES);
  }

  public boolean existsDatabaseConfiguration(String databaseName) {
    return this.containsKey(OHazelcastClusterMetadataManager.CONFIG_DATABASE_PREFIX + databaseName);
  }

  public void setDatabaseConfiguration(String databaseName, ODocument document) {
    this.put(OHazelcastClusterMetadataManager.CONFIG_DATABASE_PREFIX + databaseName, document);
  }

  public ODocument getDatabaseConfiguration(String databaseName) {
    return (ODocument)
        this.get(OHazelcastClusterMetadataManager.CONFIG_DATABASE_PREFIX + databaseName);
  }

  // Returns name of distributed databases in the cluster.
  public Set<String> getDatabases() {
    final Set<String> dbs = new HashSet<>();
    for (String key : keySet()) {
      if (key.startsWith(OHazelcastClusterMetadataManager.CONFIG_DATABASE_PREFIX)) {
        final String databaseName =
            key.substring(OHazelcastClusterMetadataManager.CONFIG_DATABASE_PREFIX.length());
        dbs.add(databaseName);
      }
    }
    return dbs;
  }

  public void removeDatabaseConfiguration(String databaseName) {
    this.remove(OHazelcastClusterMetadataManager.CONFIG_DATABASE_PREFIX + databaseName);
  }

  public static boolean isDatabaseConfiguration(String key) {
    return key.startsWith(OHazelcastClusterMetadataManager.CONFIG_DATABASE_PREFIX);
  }

  public void setDatabaseStatus(
      String node, String databaseName, ODistributedServerManager.DB_STATUS status) {
    put(
        OHazelcastClusterMetadataManager.CONFIG_DBSTATUS_PREFIX + node + "." + databaseName,
        status);
  }

  public void removeDatabaseStatus(String node, String databaseName) {
    remove(OHazelcastClusterMetadataManager.CONFIG_DBSTATUS_PREFIX + node + "." + databaseName);
  }

  public ODistributedServerManager.DB_STATUS getDatabaseStatus(String node, String databaseName) {
    return (ODistributedServerManager.DB_STATUS)
        get(OHazelcastClusterMetadataManager.CONFIG_DBSTATUS_PREFIX + node + "." + databaseName);
  }

  public ODistributedServerManager.DB_STATUS getCachedDatabaseStatus(
      String node, String databaseName) {
    return (ODistributedServerManager.DB_STATUS)
        getLocalCachedValue(
            OHazelcastClusterMetadataManager.CONFIG_DBSTATUS_PREFIX + node + "." + databaseName);
  }

  public static boolean isDatabaseStatus(String key) {
    return key.startsWith(OHazelcastClusterMetadataManager.CONFIG_DBSTATUS_PREFIX);
  }

  public static String getDatabaseStatusKeyValues(String key) {
    return key.substring(OHazelcastClusterMetadataManager.CONFIG_DBSTATUS_PREFIX.length());
  }
}

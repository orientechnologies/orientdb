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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IMap;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.distributed.ONodeConfig;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Optimized concurrent hash map implementation on top of Hazelcast distributed map.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OHazelcastDistributedMap {
  private final IMap<String, Object> hzMap;

  public static final String ORIENTDB_MAP = "orientdb";

  public OHazelcastDistributedMap(final HazelcastInstance hz) {
    hzMap = hz.getMap(ORIENTDB_MAP);
  }

  public IMap<String, Object> getHazelcastMap() {
    return hzMap;
  }

  public Object get(final Object key) {
    return hzMap.get(key);
  }

  public boolean containsKey(final Object key) {
    return hzMap.containsKey(key);
  }

  public Set<Entry<String, Object>> entrySet() {
    return hzMap.entrySet();
  }

  public Object put(final String key, final Object value) {
    try {
      return hzMap.put(key, value);
    } catch (HazelcastInstanceNotActiveException e) {
      // IGNORE IT
      return null;
    }
  }

  public Object putIfAbsent(final String key, final Object value) {
    try {
      return hzMap.putIfAbsent(key, value);
    } catch (HazelcastInstanceNotActiveException e) {
      // IGNORE IT
      return null;
    }
  }

  public Object remove(final Object key) {
    try {
      return hzMap.remove(key);
    } catch (HazelcastInstanceNotActiveException e) {
      // IGNORE IT
      return null;
    }
  }

  public boolean remove(final Object key, final Object value) {
    try {
      return hzMap.remove(key, value);
    } catch (HazelcastInstanceNotActiveException e) {
      // IGNORE IT
      return false;
    }
  }

  public void destroy() {}

  public boolean existsNode(String nodeUuid) {
    return this.containsKey(OHazelcastClusterMetadataManager.CONFIG_NODE_PREFIX + nodeUuid);
  }

  public ONodeConfig getNodeConfig(String nodeUuid) {
    ODocument doc = (ODocument) get(OHazelcastClusterMetadataManager.CONFIG_NODE_PREFIX + nodeUuid);
    if (doc == null) return null;
    return new ONodeConfig(doc);
  }

  public void removeNode(String nodeUuid) {
    this.remove(OHazelcastClusterMetadataManager.CONFIG_NODE_PREFIX + nodeUuid);
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

  public void putNodeConfig(String nodeUuid, ONodeConfig cfg) {
    put(OHazelcastClusterMetadataManager.CONFIG_NODE_PREFIX + nodeUuid, cfg.getConfig());
  }

  public Set<String> getNodeUuidByName(String name) {
    Set<String> uuids = new HashSet<String>();
    for (Iterator<Map.Entry<String, Object>> it = this.entrySet().iterator(); it.hasNext(); ) {
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
}

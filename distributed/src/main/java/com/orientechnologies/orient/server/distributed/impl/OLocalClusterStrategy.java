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
package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionStrategy;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Distributed cluster selection strategy that always prefers local cluster if any reducing network latency of remote calls. It
 * computes the best cluster the first time and every-time the configuration changes.
 * 
 * Starting from v2.2.0 a local round robin strategy is used.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OLocalClusterStrategy implements OClusterSelectionStrategy {
  public final static String              NAME           = "local";

  private OClass                          cls;
  private final ODistributedServerManager manager;
  private final String                    nodeName;
  private final String                    databaseName;
  private final AtomicLong                pointer        = new AtomicLong(0);
  private int                             lastVersion    = -1;
  private volatile List<Integer>          bestClusterIds = new ArrayList<Integer>();

  public OLocalClusterStrategy(final ODistributedServerManager iManager, final String iDatabaseName, final OClass iClass) {
    this.manager = iManager;
    this.nodeName = iManager.getLocalNodeName();
    this.databaseName = iDatabaseName;
    this.cls = iClass;
  }

  @Override
  public int getCluster(final OClass iClass, final ODocument doc) {
    if (!iClass.equals(cls))
      throw new IllegalArgumentException("Class '" + iClass + "' is different than the configured one: " + cls);

    if (bestClusterIds.isEmpty())
      readConfiguration();
    else {
      final ODistributedConfiguration cfg = manager.getDatabaseConfiguration(databaseName);
      if (lastVersion != cfg.getVersion()) {
        // DISTRIBUTED CFG IS CHANGED: GET BEST CLUSTER AGAIN
        readConfiguration();
      }
    }

    final int size = bestClusterIds.size();
    if (size == 0)
      return -1;

    if (size == 1)
      // ONLY ONE: RETURN THE FIRST ONE
      return bestClusterIds.get(0);

    return bestClusterIds.get((int) (pointer.getAndIncrement() % size));
  }

  @Override
  public String getName() {
    return NAME;
  }

  protected void readConfiguration() {
    if (cls.isAbstract())
      throw new IllegalArgumentException("Cannot create a new instance of abstract class");

    final ODatabaseDocument db = ODatabaseRecordThreadLocal.INSTANCE.get();

    final int[] clusterIds = cls.getClusterIds();
    final List<String> clusterNames = new ArrayList<String>(clusterIds.length);
    for (int c : clusterIds)
      clusterNames.add(db.getClusterNameById(c).toLowerCase());

    final ODistributedConfiguration cfg = manager.getDatabaseConfiguration(databaseName);

    final List<String> bestClusters = cfg.getOwnedClustersByServer(clusterNames, nodeName);
    if (bestClusters.isEmpty()) {

      // FILL THE MAP CLUSTER/SERVERS
      final StringBuilder buffer = new StringBuilder();
      for (String c : clusterNames) {
        if (buffer.length() > 0)
          buffer.append(" ");

        buffer.append(" ");
        buffer.append(c);
        buffer.append(":");
        buffer.append(cfg.getServers(c, null));
      }

      ODistributedServerLog.warn(this, manager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
          "Cannot find best cluster for class '%s'. Configured servers for clusters %s are %s (dCfgVersion=%d)", cls.getName(),
          clusterNames, buffer.toString(), cfg.getVersion());

      throw new ODatabaseException("Cannot find best cluster for class '" + cls.getName() + "' on server '" + nodeName
          + "'. ClusterStrategy=" + getName() + " dCfgVersion=" + cfg.getVersion());
    }

    final List<Integer> newBestClusters = new ArrayList<Integer>();
    for (String c : bestClusters)
      newBestClusters.add(db.getClusterIdByName(c));
    bestClusterIds = newBestClusters;

    lastVersion = cfg.getVersion();
  }
}

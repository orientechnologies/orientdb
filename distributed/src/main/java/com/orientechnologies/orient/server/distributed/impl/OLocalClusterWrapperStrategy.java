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
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionStrategy;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.distributed.ODistributedConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedServerLog;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Distributed cluster selection strategy as wrapper for underlying strategies. It limitates the selection of clusters to the
 * available ones on current server.
 *
 * @author Luca Garulli (l.garulli--at--orientdb.com)
 */
public class OLocalClusterWrapperStrategy implements OClusterSelectionStrategy {
  private       OClass                    cls;
  private final ODistributedServerManager manager;
  private final String                    nodeName;
  private final String                    databaseName;
  private int lastVersion = -1;
  private OClusterSelectionStrategy wrapped;
  private OLocalScopedClass         localScopedClass;

  private class OLocalScopedClass extends OClassImpl {
    public          OClassImpl wrapped;
    public volatile int[]      bestClusterIds;

    public OLocalScopedClass(final OClassImpl wrapping, final int[] newBestClusters) {
      super(wrapping.getOwner(), wrapping.getName());
      wrapped = wrapping;
      bestClusterIds = newBestClusters;
    }

    @Override
    public int[] getClusterIds() {
      return bestClusterIds;
    }
  }

  public OLocalClusterWrapperStrategy(final ODistributedServerManager iManager, final String iDatabaseName, final OClass iClass,
      final OClusterSelectionStrategy wrapped) {
    this.manager = iManager;
    this.nodeName = iManager.getLocalNodeName();
    this.databaseName = iDatabaseName;
    this.cls = iClass;
    this.localScopedClass = null;
    this.wrapped = wrapped;
  }

  @Override
  public int getCluster(final OClass iClass, final ODocument doc) {
    if (!iClass.equals(cls))
      throw new IllegalArgumentException("Class '" + iClass + "' is different than the configured one: " + cls);

    final OStorage storage = ODatabaseRecordThreadLocal.INSTANCE.get().getStorage();
    if (!(storage instanceof ODistributedStorage))
      throw new IllegalStateException("Storage is not distributed");

    if (localScopedClass == null)
      readConfiguration();
    else {
      if (lastVersion != ((ODistributedStorage) storage).getConfigurationUpdated()) {
        // DISTRIBUTED CFG IS CHANGED: GET BEST CLUSTER AGAIN
        readConfiguration();

        ODistributedServerLog.info(this, manager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
            "New cluster list for class '%s': %s (dCfgVersion=%d)", cls.getName(), localScopedClass.bestClusterIds, lastVersion);
      }
    }

    final int size = localScopedClass.bestClusterIds.length;
    if (size == 0)
      return -1;

    if (size == 1)
      // ONLY ONE: RETURN IT
      return localScopedClass.bestClusterIds[0];

    final int cluster = wrapped.getCluster(localScopedClass, doc);

    if (ODistributedServerLog.isDebugEnabled())
      ODistributedServerLog.debug(this, manager.getLocalNodeName(), null, ODistributedServerLog.DIRECTION.NONE,
          "%d Selected cluster %d for class '%s' from %s (dCfgVersion=%d)", Thread.currentThread().getId(), cluster, cls.getName(),
          Arrays.toString(localScopedClass.bestClusterIds), lastVersion);

    return cluster;
  }

  @Override
  public String getName() {
    return wrapped.getName();
  }

  protected ODistributedConfiguration readConfiguration() {
    if (cls.isAbstract())
      throw new IllegalArgumentException("Cannot create a new instance of abstract class");

    final ODatabaseDocument db = ODatabaseRecordThreadLocal.INSTANCE.get();

    final int[] clusterIds = cls.getClusterIds();
    final List<String> clusterNames = new ArrayList<String>(clusterIds.length);
    for (int c : clusterIds)
      clusterNames.add(db.getClusterNameById(c).toLowerCase());

    ODistributedConfiguration cfg = manager.getDatabaseConfiguration(databaseName);

    List<String> bestClusters = cfg.getOwnedClustersByServer(clusterNames, nodeName);
    if (bestClusters.isEmpty()) {
      // REBALANCE THE CLUSTERS
      manager.reassignClustersOwnership(nodeName, databaseName, cfg.modify());

      cfg = manager.getDatabaseConfiguration(databaseName);

      bestClusters = cfg.getOwnedClustersByServer(clusterNames, nodeName);

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

        throw new ODatabaseException(
            "Cannot find best cluster for class '" + cls.getName() + "' on server '" + nodeName + "'. ClusterStrategy=" + getName()
                + " dCfgVersion=" + cfg.getVersion());
      }
    }

    db.activateOnCurrentThread();

    final int[] newBestClusters = new int[bestClusters.size()];
    int i = 0;
    for (String c : bestClusters)
      newBestClusters[i++] = db.getClusterIdByName(c);

    this.localScopedClass = new OLocalScopedClass((OClassImpl) cls, newBestClusters);

    final ODistributedStorage storage = (ODistributedStorage) manager.getStorage(databaseName);
    lastVersion = storage.getConfigurationUpdated();

    return cfg;
  }
}

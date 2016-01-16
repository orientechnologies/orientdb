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
package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.OClusterSelectionStrategy;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.ArrayList;
import java.util.List;

/**
 * Distributed cluster selection strategy that always prefers local cluster if any reducing network latency of remote calls. It
 * computes the best cluster the first time and every-time the configuration changes.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OLocalClusterStrategy implements OClusterSelectionStrategy {
  public final static String                NAME          = "local";
  protected final OClass                    cls;
  protected final ODistributedServerManager manager;
  protected final String                    nodeName;
  protected final String                    databaseName;
  protected volatile int                    bestClusterId = -1;

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

    if (bestClusterId == -1)
      readConfiguration();

    return bestClusterId;
  }

  public void resetConfiguration() {
    bestClusterId = -1;
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

    final String bestCluster = cfg.getLocalCluster(clusterNames, nodeName);
    if (bestCluster == null) {

      // FILL THE MAP CLUSTER/SERVERS
      final StringBuilder buffer = new StringBuilder();
      for (String c : clusterNames) {
        if (buffer.length() > 0)
          buffer.append(" ");

        buffer.append("cluster ");
        buffer.append(c);
        buffer.append(": ");
        buffer.append(cfg.getServers(c, null));
      }

      OLogManager.instance().warn(this, "Cannot find best cluster for class '%s'. Configured servers for clusters %s are %s",
          cls.getName(), clusterNames, buffer.toString());

      throw new OException(
          "Cannot find best cluster for class '" + cls.getName() + "' on server '" + nodeName + "'. ClusterStrategy=" + getName());
    }

    bestClusterId = db.getClusterIdByName(bestCluster);
  }
}

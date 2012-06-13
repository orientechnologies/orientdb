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

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.MultiTask;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.server.OClientConnectionManager;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedAbstractPlugin;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ODistributedTask.OPERATION;
import com.orientechnologies.orient.server.distributed.OStorageSynchronizer;
import com.orientechnologies.orient.server.distributed.OSynchronizationLog;
import com.orientechnologies.orient.server.network.OServerNetworkListener;

/**
 * Hazelcast implementation for clustering.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHazelcastPlugin extends ODistributedAbstractPlugin implements MembershipListener {
  private String                   configFile         = "hazelcast.xml";
  private Map<String, Member>      remoteClusterNodes = new ConcurrentHashMap<String, Member>();
  private long                     timeOffset;
  private static HazelcastInstance hazelcastInstance;

  public static HazelcastInstance getHazelcastInstance() {
    return hazelcastInstance;
  }

  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
    super.config(oServer, iParams);

    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("configuration"))
        configFile = OSystemVariableResolver.resolveSystemVariables(param.value);
    }
  }

  @Override
  public void startup() {
    if (!enabled)
      return;

    try {
      hazelcastInstance = Hazelcast.init(new FileSystemXmlConfig(configFile));

      // COMPUTE THE DELTA BETWEEN LOCAL AND CENTRAL CLUSTER TIMES
      timeOffset = System.currentTimeMillis() - getHazelcastInstance().getCluster().getClusterTime();

      publishNodeConfiguration();

      // INIT THE STORAGE REPLICATORS
      replicators.clear();
      final File replicationDirectory = new File(
          OSystemVariableResolver.resolveSystemVariables(OSynchronizationLog.REPLICATION_DIRECTORY));
      if (!replicationDirectory.exists())
        replicationDirectory.mkdirs();
      else if (replicationDirectory.isDirectory()) {
        for (File f : replicationDirectory.listFiles())
          createDatabaseSynchronizer(f.getName());
      }

      // REGISTER CURRENT MEMBERS
      remoteClusterNodes.clear();
      Hazelcast.getCluster().addMembershipListener(this);
      for (Member m : Hazelcast.getCluster().getMembers())
        addMember(m);

      super.startup();

    } catch (FileNotFoundException e) {
      throw new OConfigurationException("Error on creating Hazelcast instance", e);
    }
  }

  @Override
  public void shutdown() {
    if (!enabled)
      return;

    super.shutdown();

    remoteClusterNodes.clear();
    Hazelcast.getCluster().removeMembershipListener(this);
  }

  public Object executeOperation(final String iNodeId, final OPERATION iOperation, final String dbName, final ORecordId rid,
      final int iVersion, final ORawBuffer record, final EXECUTION_MODE iMode) {
    final Member clusterMember = remoteClusterNodes.get(iNodeId);

    if (clusterMember == null)
      throw new ODistributedException("Remote node '" + iNodeId + "' is not configured");

    OLogManager.instance().debug(this, "DISTRIBUTED -> %s %s %s{%s}", iNodeId, iOperation, dbName, rid);

    final DistributedTask<Object> task = new DistributedTask<Object>(new OHazelcastReplicationTask(dbName, iOperation, rid,
        record != null ? record.buffer : null, iVersion, record != null ? record.recordType : null, iMode), clusterMember);
    Hazelcast.getExecutorService().execute(task);

    try {
      return task.get();
    } catch (Exception e) {
      OLogManager.instance().error(this, "DISTRIBUTED -> error on execution of operation", e);
      throw new ODistributedException("Error on executing remote operation against node '" + iNodeId + "'", e);
    }
  }

  @SuppressWarnings("unchecked")
  public Collection<Object> executeOperation(final Set<String> iNodeIds, final OPERATION iOperation, final String dbName,
      final ORecordId rid, final int iVersion, final ORawBuffer record, final EXECUTION_MODE iMode) throws ODistributedException {
    final Set<Member> members = new HashSet<Member>();
    for (String nodeId : iNodeIds) {
      final Member m = remoteClusterNodes.get(nodeId);
      if (m == null)
        OLogManager.instance().warn(this, "DISTRIBUTED -> cannot execute operation on remote member %s because is disconnected",
            nodeId);
      else
        members.add(m);
    }

    OLogManager.instance().debug(this, "DISTRIBUTED -> %s %s %s{%s}", members, iOperation, dbName, rid);

    final MultiTask<Object> task = new MultiTask<Object>(new OHazelcastReplicationTask(dbName, iOperation, rid, record.buffer,
        iVersion, record.recordType, iMode), members);

    if (iMode == EXECUTION_MODE.SYNCHRONOUS)
      try {
        Hazelcast.getExecutorService().execute(task);
        // CHECK FOR CONFLICTS
        Collection<Object> result = task.get();
        return result;
      } catch (Exception e) {
        OLogManager.instance().error(this, "DISTRIBUTED -> error on execution of SYNCH operation against nodes: %s", members, e);
        throw new ODistributedException("Error on executing remote operation against nodes: " + members, e);
      }
    else if (iMode == EXECUTION_MODE.ASYNCHRONOUS)
      try {
        task.setExecutionCallback(new ExecutionCallback<Collection<Object>>() {
          @Override
          public void done(Future<Collection<Object>> future) {
            try {
              if (!future.isCancelled()) {
                // CHECK FOR CONFLICTS
                Collection<Object> result = future.get();
              }
            } catch (Exception e) {
              OLogManager.instance().error(this, "DISTRIBUTED -> error on execution of ASYNCH operation against nodes: %s",
                  members, e);
            }
          }
        });
        Hazelcast.getExecutorService().execute(task);
      } catch (Exception e) {
        OLogManager.instance().error(this, "DISTRIBUTED -> error on execution of operation against nodes: %s", members, e);
        throw new ODistributedException("Error on executing remote operation against nodes: " + members, e);
      }

    return null;
  }

  @Override
  public ODocument getDatabaseConfiguration(String iDatabaseName) {
    // SEARCH IN THE CLUSTER'S DISTRIBUTED CONFIGURATION
    final IMap<String, Object> distributedConfiguration = getConfigurationMap();
    ODocument cfg = (ODocument) distributedConfiguration.get("db." + iDatabaseName);

    if (cfg == null) {
      cfg = super.getDatabaseConfiguration(iDatabaseName);
      // STORE IT IN THE CLUSTER CONFIGURATION
      distributedConfiguration.put("db." + iDatabaseName, cfg);
    }
    return cfg;
  }

  @Override
  public ODocument getDatabaseStatus(final String iDatabaseName) {
    final ODocument status = new ODocument();
    status.field("configuration", getDatabaseConfiguration(iDatabaseName), OType.EMBEDDED);
    status.field("cluster", getClusterConfiguration(), OType.EMBEDDED);
    return status;
  }

  @Override
  public ODocument getClusterConfiguration() {
    final ODocument cluster = new ODocument();

    final HazelcastInstance instance = getHazelcastInstance();

    cluster.field("name", instance.getName());
    cluster.field("local", getNodeId(instance.getCluster().getLocalMember()));

    // INSERT MEMBERS
    final List<ODocument> members = new ArrayList<ODocument>();
    cluster.field("members", members, OType.EMBEDDEDLIST);
    members.add(getLocalNodeConfiguration());
    for (Member member : remoteClusterNodes.values()) {
      members.add((ODocument) getConfigurationMap().get("node." + getNodeId(member)));
    }

    return cluster;
  }

  @Override
  public ODocument getLocalNodeConfiguration() {
    final ODocument nodeCfg = new ODocument();

    nodeCfg.field("id", getLocalNodeId());

    List<Map<String, Object>> listeners = new ArrayList<Map<String, Object>>();
    nodeCfg.field("listeners", listeners, OType.EMBEDDEDLIST);

    for (OServerNetworkListener listener : OServerMain.server().getNetworkListeners()) {
      final Map<String, Object> listenerCfg = new HashMap<String, Object>();
      listeners.add(listenerCfg);

      listenerCfg.put("protocol", listener.getProtocolType().getSimpleName());
      listenerCfg.put("listen", listener.getListeningAddress());
    }
    return nodeCfg;
  }

  public String getLocalNodeId() {
    return getNodeId(Hazelcast.getCluster().getLocalMember());
  }

  public String getLocalNodeAlias() {
    if (alias != null)
      return alias;

    return getLocalNodeId();
  }

  protected String getNodeId(final Member iMember) {
    return iMember.getInetSocketAddress().toString().substring(1);
  }

  public Set<String> getRemoteNodeIds() {
    return remoteClusterNodes.keySet();
  }

  @Override
  public void memberAdded(final MembershipEvent iEvent) {
    addMember(iEvent.getMember());
  }

  public void addMember(final Member clusterMember) {
    final String nodeId = getNodeId(clusterMember);
    // ALIGN THE ALL THE CONFIGURED DATABASES
    if (getLocalNodeId().equals(nodeId)) {
      // IT'S ME
      return;
    }

    OLogManager.instance().warn(this, "DISTRIBUTED -> connected cluster node %s, start aligning...", nodeId);

    remoteClusterNodes.put(nodeId, clusterMember);

    int aligned = 0;
    for (OStorageSynchronizer r : replicators.values()) {
      OLogManager.instance().warn(this, "DISTRIBUTED -> storage %s: aligning records...", r.toString());
      final int[] a = r.align(nodeId);
      final int tot = a[0] + a[1] + a[2];
      OLogManager.instance().warn(this, "DISTRIBUTED -> storage %s: aligned %d records (%d created, %d updated, %d deleted)",
          r.toString(), tot, a[0], a[1], a[2]);
      aligned += tot;
    }

    OLogManager.instance().warn(this, "DISTRIBUTED -> alignment completed against cluster node %s. Total records: %d", nodeId,
        aligned);

    OClientConnectionManager.instance().pushDistribCfg2Clients(getClusterConfiguration());
  }

  @Override
  public void memberRemoved(final MembershipEvent iEvent) {
    final Member clusterMember = iEvent.getMember();
    final String nodeId = getNodeId(clusterMember);

    OLogManager.instance().warn(this, "DISTRIBUTED -> disconnected cluster node %s", nodeId);
    remoteClusterNodes.remove(nodeId);

    OClientConnectionManager.instance().pushDistribCfg2Clients(getClusterConfiguration());
  }

  public long getTimeOffset() {
    return timeOffset;
  }

  protected void publishNodeConfiguration() {
    getConfigurationMap().put("node." + getLocalNodeId(), getLocalNodeConfiguration());
  }

  protected IMap<String, Object> getConfigurationMap() {
    return Hazelcast.getMap("orientdb");
  }
}

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
import java.util.Collection;
import java.util.HashSet;
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
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedAbstractPlugin;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.replication.OReplicationLog;
import com.orientechnologies.orient.server.replication.OStorageReplicator;

/**
 * Hazelcast implementation for clustering.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHazelcastPlugin extends ODistributedAbstractPlugin implements MembershipListener {
  private String                   configFile     = "hazelcast.xml";
  private Map<String, Member>      clusterMembers = new ConcurrentHashMap<String, Member>();
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

      // INIT THE STORAGE REPLICATORS
      replicators.clear();
      final File replicationDirectory = new File(
          OSystemVariableResolver.resolveSystemVariables(OReplicationLog.REPLICATION_DIRECTORY));
      if (!replicationDirectory.exists())
        replicationDirectory.mkdirs();
      else if (replicationDirectory.isDirectory()) {
        for (File f : replicationDirectory.listFiles())
          createReplicator(f.getName());
      }

      // REGISTER CURRENT MEMBERS
      clusterMembers.clear();
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

    clusterMembers.clear();
    Hazelcast.getCluster().removeMembershipListener(this);
  }

  public Object executeOperation(final String iNodeId, final byte iOperation, final String dbName, final ORecordId rid,
      final int iVersion, final ORawBuffer record, final EXECUTION_MODE iMode) {
    final Member clusterMember = clusterMembers.get(iNodeId);

    if (clusterMember == null)
      throw new ODistributedException("Remote node '" + iNodeId + "' is not configured");

    OLogManager.instance().debug(this, "DISTRIBUTED -> %s %s %s{%s}", iNodeId, ORecordOperation.getName(iOperation), dbName, rid);

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
  public Collection<Object> executeOperation(final Set<String> iNodeIds, final byte iOperation, final String dbName,
      final ORecordId rid, final int iVersion, final ORawBuffer record, final EXECUTION_MODE iMode) throws ODistributedException {
    final Set<Member> members = new HashSet<Member>();
    for (String nodeId : iNodeIds) {
      final Member m = clusterMembers.get(nodeId);
      if (m == null)
        OLogManager.instance().warn(this, "DISTRIBUTED -> cannot execute operation on remote member %s because is disconnected",
            nodeId);
      else
        members.add(m);
    }

    OLogManager.instance().debug(this, "DISTRIBUTED -> %s %s %s{%s}", members, ORecordOperation.getName(iOperation), dbName, rid);

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

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.server.distributed.ODistributedAbstractPlugin#getDatabaseConfiguration(java.lang.String)
   */
  @Override
  public ODocument getDatabaseConfiguration(String iDatabaseName) {
    // SEARCH IN THE CLUSTER'S DISTRIBUTED CONFIGURATION
    final IMap<String, String> distributedConfiguration = Hazelcast.getMap("orientdb");
    final String jsonCfg = distributedConfiguration.get("db." + iDatabaseName);

    ODocument cfg = null;
    if (jsonCfg != null)
      cfg = new ODocument().fromJSON(jsonCfg);
    else {
      cfg = super.getDatabaseConfiguration(iDatabaseName);
      // STORE IT IN THE CLUSTER CONFIGURATION
      distributedConfiguration.put("db." + iDatabaseName, cfg.toJSON());
    }
    return cfg;
  }

  public String getLocalNodeId() {
    if (alias != null)
      return alias;

    return getNodeId(Hazelcast.getCluster().getLocalMember());
  }

  protected String getNodeId(final Member iMember) {
    return iMember.getInetSocketAddress().toString().substring(1);
  }

  public ODocument getClusterConfiguration() {
    return new ODocument().field("result", Hazelcast.getCluster().toString().replace('\n', ' ').replace('\t', ' '));
  }

  public Set<String> getRemoteNodeIds() {
    return clusterMembers.keySet();
  }

  @Override
  public void memberAdded(final MembershipEvent iEvent) {
    addMember(iEvent.getMember());
  }

  public void addMember(final Member clusterMember) {
    final String nodeId = getNodeId(clusterMember);
    // ALIGN THE ALL THE CONFIGURED DATABASES
    if (getLocalNodeId().equals(nodeId))
      // IT'S ME
      return;

    OLogManager.instance().warn(this, "DISTRIBUTED -> connected cluster node %s, start aligning...", nodeId);

    clusterMembers.put(nodeId, clusterMember);

    int aligned = 0;
    for (OStorageReplicator r : replicators.values()) {
      OLogManager.instance().warn(this, "DISTRIBUTED -> storage %s: aligning records...", r.toString());
      final int[] a = r.align(nodeId);
      final int tot = a[0] + a[1] + a[2];
      OLogManager.instance().warn(this, "DISTRIBUTED -> storage %s: aligned %d records (%d created, %d updated, %d deleted)",
          r.toString(), tot, a[0], a[1], a[2]);
      aligned += tot;
    }

    OLogManager.instance().warn(this, "DISTRIBUTED -> alignment completed against cluster node %s. Total records: %d", nodeId,
        aligned);
  }

  @Override
  public void memberRemoved(final MembershipEvent iEvent) {
    final Member clusterMember = iEvent.getMember();
    final String nodeId = getNodeId(clusterMember);

    OLogManager.instance().warn(this, "DISTRIBUTED -> disconnected cluster node %s", nodeId);
    clusterMembers.remove(nodeId);
  }
}

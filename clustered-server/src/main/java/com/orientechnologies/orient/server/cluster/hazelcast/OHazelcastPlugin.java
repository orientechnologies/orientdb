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
package com.orientechnologies.orient.server.cluster.hazelcast;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.core.DistributedTask;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.MultiTask;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.cluster.OStorageReplicator;
import com.orientechnologies.orient.server.cluster.log.OReplicationLog;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.OServerCluster;
import com.orientechnologies.orient.server.handler.OServerHandlerAbstract;

/**
 * Hazelcast implementation for clustering.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHazelcastPlugin extends OServerHandlerAbstract implements OServerCluster, ODatabaseLifecycleListener,
    MembershipListener, ORecordHook {
  private boolean                         enabled               = true;
  private String                          configFile            = "hazelcast.xml";
  private long                            offlineBuffer         = -1;
  private Map<String, ODocument>          databaseConfiguration = new ConcurrentHashMap<String, ODocument>();
  private Map<String, OStorageReplicator> replicators           = new ConcurrentHashMap<String, OStorageReplicator>();
  private Map<String, Member>             clusterMembers        = new ConcurrentHashMap<String, Member>();

  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("enabled")) {
        if (!Boolean.parseBoolean(param.value)) {
          // DISABLE IT
          enabled = false;
          return;
        }
      } else if (param.name.equalsIgnoreCase("configuration"))
        configFile = OSystemVariableResolver.resolveSystemVariables(param.value);
      else if (param.name.startsWith("db."))
        databaseConfiguration.put(param.name.substring("db.".length()), (ODocument) new ODocument().fromJSON(param.value.trim()));
    }

    // CHECK THE CONFIGURATION
    if (!databaseConfiguration.containsKey("*"))
      throw new OConfigurationException("Invalid cluster configuration: cannot find settings for the default replication as 'db.*'");
  }

  @Override
  public void startup() {
    if (!enabled)
      return;

    super.startup();
    try {
      Hazelcast.init(new FileSystemXmlConfig(configFile));

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

      Orient.instance().addDbLifecycleListener(this);

    } catch (FileNotFoundException e) {
      throw new OConfigurationException("Error on creating Hazelcast instance", e);
    }
  }

  @Override
  public void shutdown() {
    if (!enabled)
      return;

    Orient.instance().removeDbLifecycleListener(this);

    clusterMembers.clear();
    Hazelcast.getCluster().removeMembershipListener(this);
    super.shutdown();
  }

  /**
   * Auto register myself as hook.
   */
  @Override
  public void onOpen(final ODatabase iDatabase) {
    final ODocument cfg = getDatabaseConfiguration(iDatabase.getName());
    if ((Boolean) cfg.field("replication")) {
      createReplicator(iDatabase.getName());

      if (iDatabase instanceof ODatabaseComplex<?>)
        ((ODatabaseComplex<?>) iDatabase).registerHook(this);
    }
  }

  /**
   * Remove myself as hook.
   */
  @Override
  public void onClose(final ODatabase iDatabase) {
    if (iDatabase instanceof ODatabaseComplex<?>)
      ((ODatabaseComplex<?>) iDatabase).unregisterHook(this);
  }

  @Override
  public boolean onTrigger(final TYPE iType, final ORecord<?> iRecord) {
    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.get();

    final OStorageReplicator replicator = replicators.get(db.getName());
    if (replicator != null)
      return replicator.distributeOperation(iType, iRecord);

    return false;
  }

  @Override
  public void sendShutdown() {
    super.sendShutdown();
  }

  @Override
  public String getName() {
    return "cluster";
  }

  public Object executeOperation(final String iNodeId, final byte iOperation, final String dbName, final ORecordId rid,
      final ORawBuffer record) {
    final Member clusterMember = clusterMembers.get(iNodeId);

    if (clusterMember == null)
      throw new ODistributedException("Remote node '" + iNodeId + "' is not configured");

    OLogManager.instance().debug(this, "DISTRIBUTED -> %s %s %s{%s}", iNodeId, ORecordOperation.getName(iOperation), dbName, rid);

    final DistributedTask<Object> task = new DistributedTask<Object>(new OReplicationTask(dbName, iOperation, rid, record.buffer,
        record.version, record.recordType), clusterMember);
    Hazelcast.getExecutorService().execute(task);

    try {
      return task.get();
    } catch (Exception e) {
      OLogManager.instance().error(this, "DISTRIBUTED -> error on execution of operation", e);
      throw new ODistributedException("Error on executing remote operation against node '" + iNodeId + "'", e);
    }
  }

  public Collection<Object> executeOperation(final Set<String> iNodeIds, final byte iOperation, final String dbName,
      final ORecordId rid, final ORawBuffer record) throws ODistributedException {
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

    final MultiTask<Object> task = new MultiTask<Object>(new OReplicationTask(dbName, iOperation, rid, record.buffer,
        record.version, record.recordType), members);
    Hazelcast.getExecutorService().execute(task);
    try {
      return task.get();

    } catch (Exception e) {
      OLogManager.instance().error(this, "DISTRIBUTED -> error on execution of operation against nodes: %s", members, e);
      throw new ODistributedException("Error on executing remote operation against nodes: " + members, e);
    }
  }

  public String getLocalNodeId() {
    return getNodeId(Hazelcast.getCluster().getLocalMember());
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
    for (OStorageReplicator r : replicators.values())
      aligned += r.align(nodeId);

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

  protected String getNodeId(final Member iMember) {
    return iMember.getInetSocketAddress().toString().substring(1);
  }

  public ODocument getClusterConfiguration() {
    return new ODocument().field("result", Hazelcast.getCluster().toString().replace('\n', ' ').replace('\t', ' '));
  }

  public ODocument getDatabaseConfiguration(final String iDatabaseName) {
    // SEARCH IN THE CLUSTER'S DISTRIBUTED CONFIGURATION
    final IMap<String, String> distributedConfiguration = Hazelcast.getMap("orientdb");
    final String jsonCfg = distributedConfiguration.get("db." + iDatabaseName);

    ODocument cfg = null;
    if (jsonCfg != null)
      cfg = new ODocument().fromJSON(jsonCfg);
    else {
      // NOT FOUND: GET BY CONFIGURATION ON LOCAL NODE
      cfg = databaseConfiguration.get(iDatabaseName);
      if (cfg == null)
        // NOT FOUND: GET THE DEFAULT ONE
        cfg = databaseConfiguration.get("*");

      // STORE IT IN THE CLUSTER CONFIGURATION
      distributedConfiguration.put("db." + iDatabaseName, cfg.toJSON());
    }
    return cfg;
  }

  public void setDefaultDatabaseConfiguration(final String iDatabaseName, final ODocument iConfiguration) {
    databaseConfiguration.put(iDatabaseName, iConfiguration);
  }

  private void createReplicator(final String iDatabaseName) {
    if (!replicators.containsKey(iDatabaseName))
      replicators.put(iDatabaseName, new OStorageReplicator(this, iDatabaseName));
  }

  public long getOfflineBuffer() {
    return offlineBuffer;
  }

  public void setOfflineBuffer(long offlineBuffer) {
    this.offlineBuffer = offlineBuffer;
  }
}

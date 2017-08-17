package com.orientechnologies.orient.server.distributed.conflict;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.task.ORemoteTask;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by luca on 13/05/17.
 */
public class OQuorumDistributedConflictResolverTest {

  @Test
  public void winnerFound() throws Exception {
    final OQuorumDistributedConflictResolver resolver = new OQuorumDistributedConflictResolver();
    final Map<Object, List<String>> candidates = new HashMap<Object, List<String>>();

    final ODocument expectedWinnerRecord = new ODocument().fields("a", 3, "b", "yes");

    // FILL CANDIDATES
    candidates.put(new ORawBuffer(expectedWinnerRecord.toStream(), 1, ODocument.RECORD_TYPE),
        Arrays.asList("server0", "server1", "server2"));
    candidates.put(new ORawBuffer(new ODocument().fields("a", 4, "b", "yes").toStream(), 3, ODocument.RECORD_TYPE),
        OMultiValue.getSingletonList("server2"));
    candidates.put(new ORawBuffer(new ODocument().fields("a", 3, "b", "no").toStream(), 4, ODocument.RECORD_TYPE),
        OMultiValue.getSingletonList("server2"));

    final ODistributedConflictResolver.OConflictResult result = resolver
        .onConflict("testdb", "testcluster", new ORecordId(10, 3), mockDistributedManager, candidates);

    Assert.assertNotNull(result.winner);
    Assert.assertTrue(result.winner instanceof ORawBuffer);

    final ODocument winnerRecord = new ODocument().fromStream(((ORawBuffer) result.winner).buffer);

    Assert.assertTrue(winnerRecord.hasSameContentOf(expectedWinnerRecord));
    Assert.assertEquals(1, ((ORawBuffer) result.winner).version);
  }

  @Test
  public void winnerNotFound() throws Exception {
    final OQuorumDistributedConflictResolver resolver = new OQuorumDistributedConflictResolver();
    final Map<Object, List<String>> candidates = new HashMap<Object, List<String>>();

    // FILL CANDIDATES
    candidates.put(new ORawBuffer(new ODocument().fields("a", 4, "b", "yes").toStream(), 3, ODocument.RECORD_TYPE),
        OMultiValue.getSingletonList("server2"));
    candidates.put(new ORawBuffer(new ODocument().fields("a", 3, "b", "no").toStream(), 4, ODocument.RECORD_TYPE),
        OMultiValue.getSingletonList("server2"));

    final ODistributedConflictResolver.OConflictResult result = resolver
        .onConflict("testdb", "testcluster", new ORecordId(10, 3), mockDistributedManager, candidates);

    Assert.assertEquals(OContentDistributedConflictResolver.NOT_FOUND, result.winner);
  }

  private ODistributedServerManager mockDistributedManager = new ODistributedServerManager() {
    @Override
    public boolean isNodeStatusEqualsTo(String iNodeName, String iDatabaseName, DB_STATUS... statuses) {
      return false;
    }

    @Override
    public boolean isNodeAvailable(String iNodeName) {
      return false;
    }

    @Override
    public Set<String> getAvailableNodeNames(String databaseName) {
      return null;
    }

    @Override
    public String getLockManagerServer() {
      return null;
    }

    @Override
    public String getCoordinatorServer() {
      return null;
    }

    @Override
    public void waitUntilNodeOnline() throws InterruptedException {

    }

    @Override
    public void waitUntilNodeOnline(String nodeName, String databaseName) throws InterruptedException {

    }

    @Override
    public OStorage getStorage(String databaseName) {
      return null;
    }

    @Override
    public OServer getServerInstance() {
      return null;
    }

    @Override
    public boolean isEnabled() {
      return false;
    }

    @Override
    public ODistributedServerManager registerLifecycleListener(ODistributedLifecycleListener iListener) {
      return null;
    }

    @Override
    public ODistributedServerManager unregisterLifecycleListener(ODistributedLifecycleListener iListener) {
      return null;
    }

    @Override
    public Object executeOnLocalNode(ODistributedRequestId reqId, ORemoteTask task, ODatabaseDocumentInternal database) {
      return null;
    }

    @Override
    public ORemoteServerController getRemoteServer(String nodeName) throws IOException {
      return null;
    }

    @Override
    public Map<String, Object> getConfigurationMap() {
      return null;
    }

    @Override
    public long getLastClusterChangeOn() {
      return 0;
    }

    @Override
    public NODE_STATUS getNodeStatus() {
      return null;
    }

    @Override
    public void setNodeStatus(NODE_STATUS iStatus) {

    }

    @Override
    public boolean checkNodeStatus(NODE_STATUS string) {
      return false;
    }

    @Override
    public void removeServer(String nodeLeftName, boolean removeOnlyDynamicServers) {

    }

    @Override
    public DB_STATUS getDatabaseStatus(String iNode, String iDatabaseName) {
      return null;
    }

    @Override
    public void setDatabaseStatus(String iNode, String iDatabaseName, DB_STATUS iStatus) {

    }

    @Override
    public int getNodesWithStatus(Collection<String> iNodes, String databaseName, DB_STATUS... statuses) {
      return 0;
    }

    @Override
    public ODistributedMessageService getMessageService() {
      return null;
    }

    @Override
    public ODistributedStrategy getDistributedStrategy() {
      return null;
    }

    @Override
    public void setDistributedStrategy(ODistributedStrategy streatgy) {

    }

    @Override
    public boolean updateCachedDatabaseConfiguration(String iDatabaseName, OModifiableDistributedConfiguration cfg,
        boolean iDeployToCluster) {
      return false;
    }

    @Override
    public long getNextMessageIdCounter() {
      return 0;
    }

    @Override
    public String getNodeUuidByName(String name) {
      return null;
    }

    @Override
    public void updateLastClusterChange() {

    }

    @Override
    public void reassignClustersOwnership(String iNode, String databaseName, OModifiableDistributedConfiguration cfg,
        boolean canCreateNewClusters) {

    }

    @Override
    public boolean isNodeAvailable(String iNodeName, String databaseName) {
      return false;
    }

    @Override
    public boolean isNodeOnline(String iNodeName, String databaseName) {
      return false;
    }

    @Override
    public int getTotalNodes(String iDatabaseName) {
      return 0;
    }

    @Override
    public int getAvailableNodes(String iDatabaseName) {
      return 0;
    }

    @Override
    public int getAvailableNodes(Collection<String> iNodes, String databaseName) {
      return 0;
    }

    @Override
    public boolean isOffline() {
      return false;
    }

    @Override
    public int getLocalNodeId() {
      return 0;
    }

    @Override
    public String getLocalNodeName() {
      return null;
    }

    @Override
    public ODocument getClusterConfiguration() {
      return null;
    }

    @Override
    public String getNodeNameById(int id) {
      return null;
    }

    @Override
    public int getNodeIdByName(String node) {
      return 0;
    }

    @Override
    public ODocument getNodeConfigurationByUuid(String iNode, boolean useCache) {
      return null;
    }

    @Override
    public ODocument getLocalNodeConfiguration() {
      return null;
    }

    @Override
    public void propagateSchemaChanges(ODatabaseInternal iStorage) {

    }

    @Override
    public ODistributedConfiguration getDatabaseConfiguration(String iDatabaseName) {
      return new ODistributedConfiguration(new ODocument().fields("clusters", new ODocument(), "writeQuorum", 2));
    }

    @Override
    public ODistributedConfiguration getDatabaseConfiguration(String iDatabaseName, boolean createIfNotPresent) {
      return null;
    }

    @Override
    public ODistributedResponse sendRequest(String iDatabaseName, Collection<String> iClusterNames,
        Collection<String> iTargetNodeNames, ORemoteTask iTask, long messageId, ODistributedRequest.EXECUTION_MODE iExecutionMode,
        Object localResult, OCallable<Void, ODistributedRequestId> iAfterSentCallback,
        OCallable<Void, ODistributedResponseManager> endCallback) {
      return null;
    }

    @Override
    public ODocument getStats() {
      return null;
    }

    @Override
    public Throwable convertException(Throwable original) {
      return null;
    }

    @Override
    public List<String> getOnlineNodes(String iDatabaseName) {
      return null;
    }

    @Override
    public boolean installDatabase(boolean iStartup, String databaseName, boolean forceDeployment, boolean tryWithDeltaFirst) {
      return false;
    }

    @Override
    public ORemoteTaskFactoryManager getTaskFactoryManager() {
      return null;
    }

    @Override
    public String electNewLockManager() {
      return null;
    }

    @Override
    public Set<String> getActiveServers() {
      return null;
    }

    @Override
    public ODistributedConflictResolverFactory getConflictResolverFactory() {
      return null;
    }

    @Override
    public long getClusterTime() {
      return 0;
    }

    @Override
    public File getDefaultDatabaseConfigFile() {
      return null;
    }

    @Override
    public ODistributedLockManager getLockManagerRequester() {
      return null;
    }

    @Override
    public ODistributedLockManager getLockManagerExecutor() {
      return null;
    }

    @Override
    public <T> T executeInDistributedDatabaseLock(String databaseName, long timeoutLocking,
        OModifiableDistributedConfiguration lastCfg, OCallable<T, OModifiableDistributedConfiguration> iCallback) {
      return null;
    }

    @Override
    public boolean isWriteQuorumPresent(String databaseName) {
      return false;
    }
  };

}
package com.orientechnologies.orient.server.distributed.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.server.distributed.*;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxSuccess;
import java.util.*;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

public class ODistributedTxCoordinatorTest {

  private OStorage storage;
  private ODistributedServerManager serverManager;
  private ODistributedDatabaseImpl distributedDatabase;
  private ODatabaseDocumentDistributed databaseDocument;
  private ODistributedMessageService messageService;

  @Before
  public void setup() {
    storage = mock(OStorage.class);
    serverManager = mock(ODistributedServerManager.class);
    distributedDatabase = mock(ODistributedDatabaseImpl.class);
    databaseDocument = mock(ODatabaseDocumentDistributed.class);
    messageService = mock(ODistributedMessageService.class);
  }

  @Test
  public void tryCommitSucceeds() {
    String dbName = "testDB";
    String localNode = "node0";
    List<String> remoteNodes = Arrays.asList("node1", "node2");
    Set<String> clusters = new HashSet<>(Collections.singleton("c1"));

    ODistributedSynchronizedSequence seq = new ODistributedSynchronizedSequence(localNode, 10);
    OTransactionInternal tx = mock(OTransactionInternal.class);
    ODistributedConfiguration distributedConfig = mock(ODistributedConfiguration.class);
    ODistributedTxResponseManager responseManager = mock(ODistributedTxResponseManager.class);

    ODistributedTxCoordinator coordinator =
        new ODistributedTxCoordinator(
            storage, serverManager, distributedDatabase, messageService, 0, localNode, 5, 100);
    coordinator.setResponseManager(responseManager);

    when(storage.getName()).thenReturn(dbName);
    when(databaseDocument.getName()).thenReturn(dbName);
    when(tx.getIndexOperations()).thenReturn(new HashMap<>());
    when(distributedDatabase.nextId()).thenReturn(seq.next());
    when(serverManager.getDatabaseConfiguration(any())).thenReturn(distributedConfig);
    when(messageService.getDatabase(dbName)).thenReturn(distributedDatabase);
    when(serverManager.getNextMessageIdCounter()).thenReturn(0L);
    when(responseManager.isQuorumReached()).thenReturn(true);
    when(databaseDocument.beginDistributedTx(any(), any(), eq(tx), eq(true), anyInt()))
        .thenReturn(true);
    when(distributedDatabase.getAvailableNodesButLocal(anySet()))
        .thenReturn(new HashSet<>(remoteNodes));
    when(responseManager.getDistributedTxFinalResponse()).thenReturn(Optional.of(new OTxSuccess()));

    coordinator.commit(databaseDocument, tx, clusters);

    InOrder inOrder =
        inOrder(distributedDatabase, databaseDocument, serverManager, responseManager);

    inOrder.verify(distributedDatabase).startOperation();
    inOrder.verify(distributedDatabase).localLock(any());
    inOrder.verify(databaseDocument).beginDistributedTx(any(), any(), eq(tx), eq(true), anyInt());
    inOrder.verify(distributedDatabase).localUnlock(any());
    inOrder
        .verify(serverManager)
        .sendRequest(
            eq(dbName),
            anyCollection(),
            argThat(targetNodes -> CollectionUtils.isEqualCollection(targetNodes, remoteNodes)),
            any(),
            anyLong(),
            eq(ODistributedRequest.EXECUTION_MODE.RESPONSE),
            any(OTxSuccess.class),
            any());
    inOrder.verify(responseManager).getDistributedTxFinalResponse();
    inOrder
        .verify(serverManager)
        .sendRequest(
            eq(dbName),
            anyCollection(),
            argThat(targetNodes -> CollectionUtils.isEqualCollection(targetNodes, remoteNodes)),
            any(),
            anyLong(),
            eq(ODistributedRequest.EXECUTION_MODE.RESPONSE),
            eq(ODistributedTxCoordinator.LOCAL_RESULT_SUCCESS));
    inOrder.verify(distributedDatabase).localLock(any());
    inOrder.verify(databaseDocument).commit2pcLocal(any());
    inOrder.verify(distributedDatabase).localUnlock(any());
    inOrder.verify(distributedDatabase).endOperation();
  }
}

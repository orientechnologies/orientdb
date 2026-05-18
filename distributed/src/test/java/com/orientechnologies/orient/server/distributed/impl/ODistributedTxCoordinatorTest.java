package com.orientechnologies.orient.server.distributed.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.orientechnologies.orient.core.transaction.ODistributedSynchronizedSequence;
import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.tx.OTransactionInternal;
import com.orientechnologies.orient.distributed.db.OrientDBDistributed;
import com.orientechnologies.orient.server.distributed.ODistributedRequestId;
import com.orientechnologies.orient.server.distributed.ODistributedServerManager;
import com.orientechnologies.orient.server.distributed.impl.metadata.OSharedContextDistributed;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxSuccess;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

public class ODistributedTxCoordinatorTest {

  private ODistributedServerManager serverManager;
  private ODistributedDatabaseImpl distributedDatabase;
  private ODatabaseDocumentDistributed databaseDocument;
  private OrientDBDistributed context;
  private OSharedContextDistributed sharedContext;

  @Before
  public void setup() {
    serverManager = mock(ODistributedServerManager.class);
    distributedDatabase = mock(ODistributedDatabaseImpl.class);
    databaseDocument = mock(ODatabaseDocumentDistributed.class);
    sharedContext = mock(OSharedContextDistributed.class);
    context = mock(OrientDBDistributed.class);
  }

  @Test
  public void tryCommitSucceeds() {
    String dbName = "testDB";
    String localNode = "node0";
    List<String> remoteNodes = Arrays.asList("node1", "node2");

    ODistributedSynchronizedSequence seq =
        new ODistributedSynchronizedSequence(new ONodeId(localNode), 10);
    OTransactionInternal tx = mock(OTransactionInternal.class);
    ODistributedTxResponseManager responseManager = mock(ODistributedTxResponseManager.class);

    ODistributedTxCoordinator coordinator =
        new ODistributedTxCoordinator(
            dbName, serverManager, distributedDatabase, localNode, 5, 100);
    coordinator.setResponseManager(responseManager);

    when(context.getAvailableNodeNotLocalNames(any())).thenReturn(new HashSet<>(remoteNodes));
    when(databaseDocument.getName()).thenReturn(dbName);
    when(databaseDocument.getContext()).thenReturn(context);
    when(databaseDocument.getSharedContext()).thenReturn(sharedContext);
    when(tx.getIndexOperations()).thenReturn(new HashMap<>());
    when(sharedContext.getTransactionSequence()).thenReturn(seq);
    when(serverManager.nextRequestId()).thenReturn(new ODistributedRequestId());
    when(responseManager.isQuorumReached()).thenReturn(true);
    when(databaseDocument.beginDistributedTx(any(), any(), eq(tx), eq(true), anyInt()))
        .thenReturn(true);
    when(responseManager.getDistributedTxFinalResponse()).thenReturn(Optional.of(new OTxSuccess()));

    coordinator.commit(databaseDocument, tx);

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
            argThat(targetNodes -> CollectionUtils.isEqualCollection(targetNodes, remoteNodes)),
            any(),
            any(ODistributedRequestId.class),
            any(OTxSuccess.class),
            any());
    inOrder.verify(responseManager).getDistributedTxFinalResponse();
    inOrder
        .verify(serverManager)
        .sendRequest(
            eq(dbName),
            argThat(targetNodes -> CollectionUtils.isEqualCollection(targetNodes, remoteNodes)),
            any());
    inOrder.verify(distributedDatabase).localLock(any());
    inOrder.verify(databaseDocument).commit2pcLocal(any());
    inOrder.verify(distributedDatabase).localUnlock(any());
    inOrder.verify(distributedDatabase).endOperation();
  }
}

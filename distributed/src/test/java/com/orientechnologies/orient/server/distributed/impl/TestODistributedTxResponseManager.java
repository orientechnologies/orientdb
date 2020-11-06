package com.orientechnologies.orient.server.distributed.impl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.distributed.ODistributedTxResponseManager;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1Task;
import com.orientechnologies.orient.server.distributed.impl.task.OTransactionPhase1TaskResult;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxConcurrentModification;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxRecordLockTimeout;
import com.orientechnologies.orient.server.distributed.impl.task.transaction.OTxSuccess;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Test;

public class TestODistributedTxResponseManager {

  @Test
  public void testSimpleQuorum() {
    OTransactionPhase1Task transaction = new OTransactionPhase1Task();
    Set<String> nodes = new HashSet<>();
    nodes.add("one");
    nodes.add("two");
    nodes.add("three");
    ODistributedTxResponseManagerImpl responseManager =
        new ODistributedTxResponseManagerImpl(transaction, nodes, nodes, 3, 3, 2);
    assertFalse(
        responseManager.collectResponse(new OTransactionPhase1TaskResult(new OTxSuccess()), "one"));
    assertFalse(
        responseManager.collectResponse(new OTransactionPhase1TaskResult(new OTxSuccess()), "two"));
    assertTrue(responseManager.isQuorumReached());
  }

  @Test
  public void testSimpleNoQuorum() {
    OTransactionPhase1Task transaction = new OTransactionPhase1Task();
    Set<String> nodes = new HashSet<>();
    nodes.add("one");
    nodes.add("two");
    nodes.add("three");
    ODistributedTxResponseManager responseManager =
        new ODistributedTxResponseManagerImpl(transaction, nodes, nodes, 3, 3, 2);
    assertFalse(
        responseManager.collectResponse(new OTransactionPhase1TaskResult(new OTxSuccess()), "one"));
    assertFalse(
        responseManager.collectResponse(
            new OTransactionPhase1TaskResult(new OTxConcurrentModification(new ORecordId(1, 1), 1)),
            "two"));
    assertTrue(
        responseManager.collectResponse(
            new OTransactionPhase1TaskResult(new OTxRecordLockTimeout("s", new ORecordId(10, 10))),
            "two"));
    assertFalse(responseManager.isQuorumReached());
  }

  @Test
  public void testSimpleQuorumLocal() {
    OTransactionPhase1Task transaction = new OTransactionPhase1Task();
    Set<String> nodes = new HashSet<>();
    nodes.add("one");
    nodes.add("two");
    nodes.add("three");
    ODistributedTxResponseManager responseManager =
        new ODistributedTxResponseManagerImpl(transaction, nodes, nodes, 3, 3, 2);
    assertFalse(responseManager.setLocalResult("one", new OTxSuccess()));
    assertFalse(
        responseManager.collectResponse(
            new OTransactionPhase1TaskResult(new OTxSuccess()), "three"));
    assertTrue(responseManager.isQuorumReached());
  }

  @Test
  public void testSimpleFinishLocal() {
    OTransactionPhase1Task transaction = new OTransactionPhase1Task();
    Set<String> nodes = new HashSet<>();
    nodes.add("one");
    nodes.add("two");
    nodes.add("three");
    ODistributedTxResponseManager responseManager =
        new ODistributedTxResponseManagerImpl(transaction, nodes, nodes, 3, 3, 2);
    assertFalse(responseManager.setLocalResult("one", new OTxSuccess()));
    assertFalse(
        responseManager.collectResponse(new OTransactionPhase1TaskResult(new OTxSuccess()), "two"));
    assertTrue(
        responseManager.collectResponse(
            new OTransactionPhase1TaskResult(new OTxSuccess()), "three"));
    assertTrue(responseManager.isQuorumReached());
  }

  @Test
  public void testWaitToComplete() throws InterruptedException, ExecutionException {
    ExecutorService executor = Executors.newCachedThreadPool();

    OTransactionPhase1Task transaction = new OTransactionPhase1Task();
    Set<String> nodes = new HashSet<>();
    nodes.add("one");
    nodes.add("two");
    nodes.add("three");
    ODistributedTxResponseManager responseManager =
        new ODistributedTxResponseManagerImpl(transaction, nodes, nodes, 3, 3, 2);
    responseManager.setLocalResult("one", new OTxSuccess());
    CountDownLatch startedWaiting = new CountDownLatch(1);
    Future<Boolean> future =
        executor.submit(
            () -> {
              startedWaiting.countDown();
              return responseManager.waitForSynchronousResponses();
            });
    startedWaiting.await();
    assertFalse(future.isDone());
    responseManager.collectResponse(new OTransactionPhase1TaskResult(new OTxSuccess()), "one");
    assertTrue(future.get());
    assertTrue(responseManager.isQuorumReached());
  }

  @Test
  public void testWaitToCompleteNoQuorum() throws InterruptedException, ExecutionException {
    ExecutorService executor = Executors.newCachedThreadPool();

    OTransactionPhase1Task transaction = new OTransactionPhase1Task();
    Set<String> nodes = new HashSet<>();
    nodes.add("one");
    nodes.add("two");
    nodes.add("three");
    ODistributedTxResponseManager responseManager =
        new ODistributedTxResponseManagerImpl(transaction, nodes, nodes, 3, 3, 2);
    responseManager.collectResponse(new OTransactionPhase1TaskResult(new OTxSuccess()), "one");
    assertFalse(
        responseManager.collectResponse(
            new OTransactionPhase1TaskResult(new OTxConcurrentModification(new ORecordId(1, 1), 1)),
            "two"));
    CountDownLatch startedWaiting = new CountDownLatch(1);
    Future<Boolean> future =
        executor.submit(
            () -> {
              startedWaiting.countDown();
              return responseManager.waitForSynchronousResponses();
            });
    startedWaiting.await();
    assertFalse(future.isDone());
    assertTrue(
        responseManager.collectResponse(
            new OTransactionPhase1TaskResult(new OTxRecordLockTimeout("s", new ORecordId(10, 10))),
            "one"));
    assertFalse(future.get());
    assertFalse(responseManager.isQuorumReached());
  }
}

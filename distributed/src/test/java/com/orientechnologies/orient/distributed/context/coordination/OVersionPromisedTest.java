package com.orientechnologies.orient.distributed.context.coordination;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.transaction.ONodeId;
import com.orientechnologies.orient.core.transaction.OTransactionId;
import com.orientechnologies.orient.core.transaction.OTransactionIdPromise;
import com.orientechnologies.orient.distributed.context.coordination.result.OAlreadyPromised;
import com.orientechnologies.orient.distributed.context.coordination.result.OOutdatedVersion;
import java.util.UUID;
import org.junit.Test;

public class OVersionPromisedTest {

  private ONodeId newRandomNodeId() {
    return new ONodeId(UUID.randomUUID().toString());
  }

  private OTransactionIdPromise newPromiseId(ONodeId node) {
    return new OTransactionIdPromise(node, new OTransactionId(10, 10));
  }

  @Test
  public void testSimplePromise() {
    var node = newRandomNodeId();
    var promisedId = newPromiseId(node);
    var baseVersion = new OVersion(0);
    var promise = new OVersionPromise(baseVersion, newRandomNodeId());
    var next = baseVersion.next();
    var result = promise.promise(promisedId, next);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testPromiseAcceptOutdated() {
    var node = newRandomNodeId();
    var promisedId = newPromiseId(node);
    var baseVersion = new OVersion(0);
    var promise = new OVersionPromise(baseVersion, newRandomNodeId());
    var next = baseVersion.next();
    var result = promise.promise(promisedId, next);
    assertTrue(result.isEmpty());
    promise.accept(promisedId, next);
    result = promise.promise(promisedId, next);
    var newNext = next.next();
    result = promise.promise(promisedId, newNext);
    assertTrue(result.isEmpty());
    promise.accept(promisedId, newNext);
    promise.accept(promisedId, next);
    assertEquals(promise.getVersion(), newNext);
  }

  @Test
  public void testRecoverTransaction() {
    var node = newRandomNodeId();
    var promisedId = newPromiseId(node);
    var baseVersion = new OVersion(0);
    var promise = new OVersionPromise(baseVersion, newRandomNodeId());
    var next = baseVersion.next();
    var result = promise.promise(promisedId, next);
    assertTrue(result.isEmpty());
    var nextPromise = promisedId.retrySequence(node);
    result = promise.promise(nextPromise, next);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testRecoverTwiceTransaction() {
    var node = newRandomNodeId();
    var promisedId = newPromiseId(node);
    var baseVersion = new OVersion(0);
    var promise = new OVersionPromise(baseVersion, newRandomNodeId());
    var next = baseVersion.next();
    var result = promise.promise(promisedId, next);
    assertTrue(result.isEmpty());
    var nextPromise = promisedId.retrySequence(node);
    nextPromise = nextPromise.retrySequence(node);
    result = promise.promise(nextPromise, next);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testWrongRecoverTransaction() {
    var node = newRandomNodeId();
    var promisedId = newPromiseId(node);
    var baseVersion = new OVersion(0);
    var promise = new OVersionPromise(baseVersion, newRandomNodeId());
    var next = baseVersion.next();
    var result = promise.promise(promisedId, next);
    assertTrue(result.isEmpty());
    var nextPromise = promisedId.retrySequence(node);
    var newNextPromise = nextPromise.retrySequence(node);
    result = promise.promise(newNextPromise, next);
    assertTrue(result.isEmpty());
    result = promise.promise(nextPromise, next);
    assertTrue(result.get() instanceof OAlreadyPromised);
  }

  @Test
  public void testWrongVersionRecoverTransaction() {
    var node = newRandomNodeId();
    var promisedId = newPromiseId(node);
    var baseVersion = new OVersion(0);
    var promise = new OVersionPromise(baseVersion, newRandomNodeId());
    var next = baseVersion.next();
    var result = promise.promise(promisedId, next);
    assertTrue(result.isEmpty());
    var nextPromise = promisedId.retrySequence(node);
    var newNextPromise = nextPromise.retrySequence(node);
    result = promise.promise(newNextPromise, next.next());
    assertTrue(result.get() instanceof OOutdatedVersion);
  }

  @Test
  public void testSimpleWrongVersion() {
    var node = newRandomNodeId();
    var promisedId = newPromiseId(node);
    var baseVersion = new OVersion(0);
    var promise = new OVersionPromise(baseVersion, newRandomNodeId());
    var next = baseVersion.next();
    next = next.next();
    var result = promise.promise(promisedId, next);
    assertTrue(result.get() instanceof OOutdatedVersion);
  }
}

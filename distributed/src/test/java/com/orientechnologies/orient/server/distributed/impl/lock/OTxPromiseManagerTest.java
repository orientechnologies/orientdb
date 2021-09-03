package com.orientechnologies.orient.server.distributed.impl.lock;

import static org.junit.Assert.*;

import com.orientechnologies.orient.core.tx.OTransactionId;
import com.orientechnologies.orient.server.distributed.exception.OTxPromiseException;
import java.util.Optional;
import org.junit.Test;

public class OTxPromiseManagerTest {
  @Test
  public void testReaquirePromise() {
    OTxPromiseManager<String> mgr = new OTxPromiseManager<>();
    OTransactionId txId = new OTransactionId(Optional.of("0"), 1, 1);
    OTransactionId returnedId = mgr.promise("key1", 0, txId, false);
    assertNull(returnedId);
    assertEquals(mgr.size(), 1);
    returnedId = mgr.promise("key1", 0, txId, false);
    assertNull(returnedId);
    assertEquals(mgr.size(), 1);
    mgr.release("key1", txId);
    assertEquals(mgr.size(), 0);
  }

  @Test
  public void testForcePromise() {
    OTxPromiseManager<String> mgr = new OTxPromiseManager<>();
    OTransactionId txId1 = new OTransactionId(Optional.of("0"), 1, 1);
    OTransactionId txId2 = new OTransactionId(Optional.of("0"), 1, 2);
    OTransactionId returnedId = mgr.promise("key1", 0, txId1, false);
    assertNull(returnedId);
    assertEquals(mgr.size(), 1);
    returnedId = mgr.promise("key1", 0, txId2, true);
    assertEquals(returnedId, txId1);
    assertEquals(mgr.size(), 1);
    mgr.release("key1", txId1); // shouldn't release anything!
    assertEquals(mgr.size(), 1);
    mgr.release("key1", txId2);
    assertEquals(mgr.size(), 0);
  }

  @Test(expected = OTxPromiseException.class)
  public void testForcePromise_differentVersion() {
    OTxPromiseManager<String> mgr = new OTxPromiseManager<>();
    OTransactionId txId1 = new OTransactionId(Optional.of("0"), 1, 1);
    OTransactionId txId2 = new OTransactionId(Optional.of("0"), 1, 2);
    OTransactionId returnedId = mgr.promise("key1", 0, txId1, false);
    assertNull(returnedId);
    try {
      returnedId = mgr.promise("key1", 1, txId2, true);
    } finally {
      assertNull(returnedId);
      assertEquals(1, mgr.size());
    }
    fail("Expected an exception!");
  }
}

package com.orientechnologies.orient.server.distributed.impl.task.transaction;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class OTransactionSequenceManagerTest {

  @Test
  public void simpleSequenceGeneration() {
    OTransactionSequenceManager sequenceManager = new OTransactionSequenceManager();
    OTransactionId one = sequenceManager.next();
    OTransactionId two = sequenceManager.next();

    OTransactionSequenceManager sequenceManagerRecv = new OTransactionSequenceManager();
    Assert.assertTrue(sequenceManagerRecv.validateTransactionId(one));
    Assert.assertTrue(sequenceManagerRecv.validateTransactionId(two));

    Assert.assertNull(sequenceManager.notifySuccess(one));
    Assert.assertNull(sequenceManager.notifySuccess(two));

    Assert.assertNull(sequenceManagerRecv.notifySuccess(one));
    Assert.assertNull(sequenceManagerRecv.notifySuccess(two));
  }

  @Test
  public void sequenceMissing() {
    OTransactionSequenceManager sequenceManager = new OTransactionSequenceManager();
    OTransactionId one = sequenceManager.next();
    OTransactionId two = sequenceManager.next();
    OTransactionId three = sequenceManager.next();

    OTransactionSequenceManager sequenceManagerRecv = new OTransactionSequenceManager();
    Assert.assertTrue(sequenceManagerRecv.validateTransactionId(one));
    Assert.assertTrue(sequenceManagerRecv.validateTransactionId(three));

    Assert.assertNull(sequenceManager.notifySuccess(one));
    Assert.assertNull(sequenceManager.notifySuccess(two));
    Assert.assertNull(sequenceManager.notifySuccess(three));

    Assert.assertNull(sequenceManagerRecv.notifySuccess(one));
    Assert.assertNull(sequenceManagerRecv.notifySuccess(three));

    long[] status = sequenceManager.currentStatus();

    List<OTransactionId> list = sequenceManagerRecv.otherStatus(status);
    Assert.assertNotNull(list);
    Assert.assertTrue(list.contains(two));

  }

  @Test
  public void sequenceMissingSameSpot() {
    OTransactionSequenceManager sequenceManager = new OTransactionSequenceManager();
    OTransactionId one = sequenceManager.nextAt(1);
    Assert.assertNull(sequenceManager.notifySuccess(one));
    OTransactionId two = sequenceManager.nextAt(1);
    Assert.assertNull(sequenceManager.notifySuccess(two));
    OTransactionId three = sequenceManager.nextAt(1);
    Assert.assertNull(sequenceManager.notifySuccess(three));

    OTransactionSequenceManager sequenceManagerRecv = new OTransactionSequenceManager();
    Assert.assertTrue(sequenceManagerRecv.validateTransactionId(one));
    Assert.assertFalse(sequenceManagerRecv.validateTransactionId(three));

    Assert.assertNull(sequenceManagerRecv.notifySuccess(one));
    // This may fail in some cases as early detection
    List<OTransactionId> res = sequenceManagerRecv.notifySuccess(three);
    Assert.assertNotNull(res);
    Assert.assertTrue(res.contains(two));

    long[] status = sequenceManager.currentStatus();

    // this will for sure contain two, it may even cantain three
    List<OTransactionId> list = sequenceManagerRecv.otherStatus(status);
    Assert.assertNotNull(list);
    Assert.assertTrue(list.contains(two));
    Assert.assertTrue(list.contains(three));

  }

}

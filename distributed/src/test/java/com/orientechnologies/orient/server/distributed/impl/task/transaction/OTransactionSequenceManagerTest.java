package com.orientechnologies.orient.server.distributed.impl.task.transaction;

import com.orientechnologies.orient.server.distributed.OTransactionId;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;

public class OTransactionSequenceManagerTest {

  @Test
  public void simpleSequenceGeneration() {
    OTransactionSequenceManager sequenceManager = new OTransactionSequenceManager("one");
    OTransactionId one = sequenceManager.next().get();
    OTransactionId two = sequenceManager.next().get();

    OTransactionSequenceManager sequenceManagerRecv = new OTransactionSequenceManager("two");
    assertFalse(sequenceManagerRecv.validateTransactionId(one).isPresent());
    assertFalse(sequenceManagerRecv.validateTransactionId(two).isPresent());

    assertTrue(sequenceManager.notifySuccess(one).isEmpty());
    assertTrue(sequenceManager.notifySuccess(two).isEmpty());

    assertTrue(sequenceManagerRecv.notifySuccess(one).isEmpty());
    assertTrue(sequenceManagerRecv.notifySuccess(two).isEmpty());
  }

  @Test
  public void sequenceMissing() {
    OTransactionSequenceManager sequenceManager = new OTransactionSequenceManager("one");
    OTransactionId one = sequenceManager.next().get();
    OTransactionId two = sequenceManager.next().get();
    OTransactionId three = sequenceManager.next().get();

    OTransactionSequenceManager sequenceManagerRecv = new OTransactionSequenceManager("two");
    assertFalse(sequenceManagerRecv.validateTransactionId(one).isPresent());
    assertFalse(sequenceManagerRecv.validateTransactionId(three).isPresent());

    assertTrue(sequenceManager.notifySuccess(one).isEmpty());
    assertTrue(sequenceManager.notifySuccess(two).isEmpty());
    assertTrue(sequenceManager.notifySuccess(three).isEmpty());

    assertTrue(sequenceManagerRecv.notifySuccess(one).isEmpty());
    assertTrue(sequenceManagerRecv.notifySuccess(three).isEmpty());

    long[] status = sequenceManager.currentStatus();

    List<OTransactionId> list = sequenceManagerRecv.checkStatus(status);
    assertNotNull(list);
    assertTrue(list.contains(new OTransactionId(Optional.empty(), two.getPosition(), two.getSequence())));
  }

  @Test
  public void sequenceMissingPromised() {
    OTransactionSequenceManager sequenceManager = new OTransactionSequenceManager("one");
    OTransactionId one = sequenceManager.next().get();
    OTransactionId two = sequenceManager.next().get();
    OTransactionId three = sequenceManager.next().get();

    OTransactionSequenceManager sequenceManagerRecv = new OTransactionSequenceManager("two");
    assertFalse(sequenceManagerRecv.validateTransactionId(one).isPresent());
    assertFalse(sequenceManagerRecv.validateTransactionId(three).isPresent());

    assertTrue(sequenceManager.notifySuccess(one).isEmpty());
    assertTrue(sequenceManager.notifySuccess(two).isEmpty());
    assertTrue(sequenceManager.notifySuccess(three).isEmpty());

    assertTrue(sequenceManagerRecv.notifySuccess(one).isEmpty());

    long[] status = sequenceManager.currentStatus();

    List<OTransactionId> list = sequenceManagerRecv.checkStatus(status);
    assertNotNull(list);
    assertTrue(list.contains(new OTransactionId(Optional.empty(), two.getPosition(), two.getSequence())));
    assertEquals(list.size(), 1);
  }

  @Test
  public void sequenceMissingSameSpot() {
    OTransactionSequenceManager sequenceManager = new OTransactionSequenceManager("one");
    OTransactionId one = sequenceManager.nextAt(1);
    assertTrue(sequenceManager.notifySuccess(one).isEmpty());
    OTransactionId two = sequenceManager.nextAt(1);
    assertTrue(sequenceManager.notifySuccess(two).isEmpty());
    OTransactionId three = sequenceManager.nextAt(1);
    assertTrue(sequenceManager.notifySuccess(three).isEmpty());

    OTransactionSequenceManager sequenceManagerRecv = new OTransactionSequenceManager("two");
    assertFalse(sequenceManagerRecv.validateTransactionId(one).isPresent());
    assertTrue(sequenceManagerRecv.validateTransactionId(three).isPresent());

    assertTrue(sequenceManagerRecv.notifySuccess(one).isEmpty());
    // This may fail in some cases as early detection
    List<OTransactionId> res = sequenceManagerRecv.notifySuccess(three);
    assertNotNull(res);
    assertTrue(res.contains(new OTransactionId(Optional.empty(), two.getPosition(), two.getSequence())));

    long[] status = sequenceManager.currentStatus();

    // this will for sure contain two, it may even cantain three
    List<OTransactionId> list = sequenceManagerRecv.checkStatus(status);
    assertNotNull(list);
    assertTrue(list.contains(new OTransactionId(Optional.empty(), two.getPosition(), two.getSequence())));
    assertTrue(list.contains(new OTransactionId(Optional.empty(), three.getPosition(), three.getSequence())));
  }

  @Test
  public void sequenceMissingSameSpotValidation() {
    OTransactionSequenceManager sequenceManager = new OTransactionSequenceManager("one");
    OTransactionId one = sequenceManager.nextAt(1);
    assertTrue(sequenceManager.notifySuccess(one).isEmpty());
    OTransactionId two = sequenceManager.nextAt(1);
    assertTrue(sequenceManager.notifySuccess(two).isEmpty());
    OTransactionId three = sequenceManager.nextAt(1);
    assertTrue(sequenceManager.notifySuccess(three).isEmpty());

    OTransactionSequenceManager sequenceManagerRecv = new OTransactionSequenceManager("two");
    assertTrue(!sequenceManagerRecv.validateTransactionId(one).isPresent());
    assertTrue(sequenceManagerRecv.notifySuccess(one).isEmpty());
    assertFalse(!sequenceManagerRecv.validateTransactionId(three).isPresent());
  }

  @Test
  public void sequenceMissingSameSpotValidationBack() {
    OTransactionSequenceManager sequenceManager = new OTransactionSequenceManager("one");
    OTransactionId one = sequenceManager.nextAt(1);
    assertTrue(sequenceManager.notifySuccess(one).isEmpty());
    OTransactionId two = sequenceManager.nextAt(1);
    assertTrue(sequenceManager.notifySuccess(two).isEmpty());

    OTransactionSequenceManager sequenceManagerOther = new OTransactionSequenceManager("one");
    OTransactionId otherOne = sequenceManagerOther.nextAt(1);
    assertTrue(sequenceManager.notifySuccess(one).isEmpty());


    OTransactionSequenceManager sequenceManagerRecv = new OTransactionSequenceManager("two");
    assertTrue(!sequenceManagerRecv.validateTransactionId(one).isPresent());
    assertTrue(sequenceManagerRecv.notifySuccess(one).isEmpty());
    assertTrue(!sequenceManagerRecv.validateTransactionId(two).isPresent());
    assertTrue(sequenceManagerRecv.notifySuccess(two).isEmpty());

    assertFalse(!sequenceManagerRecv.validateTransactionId(otherOne).isPresent());
  }

  @Test
  public void sequenceMissingSameSpotMissing() {
    OTransactionSequenceManager sequenceManager = new OTransactionSequenceManager("one");
    OTransactionId one = sequenceManager.nextAt(1);
    assertTrue(sequenceManager.notifySuccess(one).isEmpty());
    OTransactionId two = sequenceManager.nextAt(1);
    assertTrue(sequenceManager.notifySuccess(two).isEmpty());
    OTransactionId three = sequenceManager.nextAt(1);
    assertTrue(sequenceManager.notifySuccess(three).isEmpty());

    OTransactionSequenceManager sequenceManagerRecv = new OTransactionSequenceManager("two");
    assertTrue(!sequenceManagerRecv.validateTransactionId(one).isPresent());
    assertTrue(sequenceManagerRecv.notifySuccess(one).isEmpty());
    assertTrue(!sequenceManagerRecv.validateTransactionId(two).isPresent());
    // This may fail in some cases as early detection
    List<OTransactionId> res = sequenceManagerRecv.notifySuccess(three);
    assertNotNull(res);
    assertTrue(res.contains(new OTransactionId(Optional.empty(), three.getPosition(), three.getSequence())));

    long[] status = sequenceManager.currentStatus();

    // this will for sure contain two, it may even cantain three
    List<OTransactionId> list = sequenceManagerRecv.checkStatus(status);
    assertNotNull(list);
    //assertTrue(list.contains(two));
    assertTrue(list.contains(new OTransactionId(Optional.empty(), three.getPosition(), three.getSequence())));

  }

  @Test
  public void simpleStoreRestore() {
    OTransactionSequenceManager sequenceManager = new OTransactionSequenceManager("one");
    OTransactionId one = sequenceManager.next().get();
    OTransactionId two = sequenceManager.next().get();
    assertTrue(sequenceManager.notifySuccess(one).isEmpty());
    assertTrue(sequenceManager.notifySuccess(two).isEmpty());
    byte[] bytes = sequenceManager.store();
    OTransactionSequenceManager readSequenceManager = new OTransactionSequenceManager("two");
    readSequenceManager.fill(bytes);

    assertArrayEquals(sequenceManager.currentStatus(), readSequenceManager.currentStatus());

  }

  @Test
  public void testAllBusy() {
    OTransactionSequenceManager sequenceManager = new OTransactionSequenceManager("one");
    for (int i = 0; i < 1000; i++) {
      sequenceManager.nextAt(i);
    }
    assertFalse(sequenceManager.next().isPresent());
  }

  @Test
  public void testNotificationFailure() {
    OTransactionSequenceManager sequenceManager = new OTransactionSequenceManager("one");
    OTransactionId one = sequenceManager.nextAt(1);
    assertTrue(sequenceManager.notifySuccess(one).isEmpty());
    OTransactionId two = sequenceManager.nextAt(1);
    assertTrue(sequenceManager.notifySuccess(two).isEmpty());
    OTransactionId three = sequenceManager.nextAt(1);
    assertTrue(sequenceManager.notifySuccess(three).isEmpty());

    OTransactionSequenceManager sequenceManagerOther = new OTransactionSequenceManager("two");
    assertFalse(sequenceManagerOther.validateTransactionId(one).isPresent());
    assertTrue(sequenceManagerOther.notifySuccess(one).isEmpty());
    assertFalse(sequenceManagerOther.validateTransactionId(two).isPresent());
    assertTrue(sequenceManagerOther.notifySuccess(two).isEmpty());

    OTransactionId otherThree = sequenceManagerOther.nextAt(1);

    OTransactionSequenceManager sequenceManagerRecv = new OTransactionSequenceManager("three");
    assertFalse(sequenceManagerRecv.validateTransactionId(one).isPresent());
    assertTrue(sequenceManagerRecv.notifySuccess(one).isEmpty());
    assertFalse(sequenceManagerRecv.validateTransactionId(two).isPresent());
    assertTrue(sequenceManagerRecv.notifySuccess(two).isEmpty());
    assertFalse(sequenceManagerRecv.validateTransactionId(three).isPresent());
    assertTrue(sequenceManagerRecv.validateTransactionId(otherThree).isPresent());
    assertFalse(sequenceManagerRecv.notifyFailure(otherThree));
    assertTrue(sequenceManagerRecv.notifyFailure(three));
    assertFalse(sequenceManagerRecv.validateTransactionId(otherThree).isPresent());
  }

}

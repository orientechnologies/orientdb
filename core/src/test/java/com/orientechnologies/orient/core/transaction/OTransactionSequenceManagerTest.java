package com.orientechnologies.orient.core.transaction;

import static com.orientechnologies.orient.core.tx.ValidationResult.ALREADY_PRESENT;
import static com.orientechnologies.orient.core.tx.ValidationResult.ALREADY_PROMISED;
import static com.orientechnologies.orient.core.tx.ValidationResult.MISSING_PREVIOUS;
import static com.orientechnologies.orient.core.tx.ValidationResult.VALID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.tx.OTransactionSequenceStatus;
import com.orientechnologies.orient.core.tx.SuccessResult;
import java.io.IOException;
import java.util.List;
import org.junit.Test;

public class OTransactionSequenceManagerTest {

  @Test
  public void simpleSequenceGeneration() {
    OTransactionSequenceManager sequenceManager =
        new OTransactionSequenceManager(new ONodeId("one"), 1000);
    OTransactionIdPromise one = sequenceManager.next().get();
    OTransactionIdPromise two = sequenceManager.next().get();

    OTransactionSequenceManager sequenceManagerRecv =
        new OTransactionSequenceManager(new ONodeId("two"), 1000);
    assertEquals(sequenceManagerRecv.validate(one), VALID);
    assertEquals(sequenceManagerRecv.validate(two), VALID);

    assertEquals(sequenceManager.notifySuccess(one), SuccessResult.VALID);
    assertEquals(sequenceManager.notifySuccess(two), SuccessResult.VALID);

    assertEquals(sequenceManagerRecv.notifySuccess(one), SuccessResult.VALID);
    assertEquals(sequenceManagerRecv.notifySuccess(two), SuccessResult.VALID);
  }

  @Test
  public void sequenceMissing() {
    OTransactionSequenceManager sequenceManager =
        new OTransactionSequenceManager(new ONodeId("one"), 1000);
    OTransactionIdPromise one = sequenceManager.next().get();
    OTransactionIdPromise two = sequenceManager.next().get();
    OTransactionIdPromise three = sequenceManager.next().get();

    OTransactionSequenceManager sequenceManagerRecv =
        new OTransactionSequenceManager(new ONodeId("two"), 1000);
    assertEquals(sequenceManagerRecv.validate(one), VALID);
    assertEquals(sequenceManagerRecv.validate(three), VALID);

    assertEquals(sequenceManager.notifySuccess(one), SuccessResult.VALID);
    assertEquals(sequenceManager.notifySuccess(two), SuccessResult.VALID);
    assertEquals(sequenceManager.notifySuccess(three), SuccessResult.VALID);

    assertEquals(sequenceManagerRecv.notifySuccess(one), SuccessResult.VALID);
    assertEquals(sequenceManagerRecv.notifySuccess(three), SuccessResult.VALID);

    OTransactionSequenceStatus status = sequenceManager.currentStatus();

    List<OTransactionId> list = sequenceManagerRecv.checkSelfStatus(status);
    assertNotNull(list);
    assertTrue(
        list.contains(new OTransactionId(two.getId().getPosition(), two.getId().getSequence())));
  }

  @Test
  public void sequenceMissingPromised() {
    OTransactionSequenceManager sequenceManager =
        new OTransactionSequenceManager(new ONodeId("one"), 1000);
    OTransactionIdPromise one = sequenceManager.next().get();
    OTransactionIdPromise two = sequenceManager.next().get();
    OTransactionIdPromise three = sequenceManager.next().get();

    OTransactionSequenceManager sequenceManagerRecv =
        new OTransactionSequenceManager(new ONodeId("two"), 1000);
    assertEquals(sequenceManagerRecv.validate(one), VALID);
    assertEquals(sequenceManagerRecv.validate(three), VALID);

    assertEquals(sequenceManager.notifySuccess(one), SuccessResult.VALID);
    assertEquals(sequenceManager.notifySuccess(two), SuccessResult.VALID);
    assertEquals(sequenceManager.notifySuccess(three), SuccessResult.VALID);

    assertEquals(sequenceManagerRecv.notifySuccess(one), SuccessResult.VALID);

    OTransactionSequenceStatus status = sequenceManager.currentStatus();

    List<OTransactionId> list = sequenceManagerRecv.checkSelfStatus(status);
    assertNotNull(list);
    assertTrue(
        list.contains(new OTransactionId(two.getId().getPosition(), two.getId().getSequence())));
    assertEquals(list.size(), 1);
  }

  @Test
  public void sequenceMissingSameSpot() {
    OTransactionSequenceManager sequenceManager =
        new OTransactionSequenceManager(new ONodeId("one"), 1000);
    OTransactionIdPromise one = sequenceManager.nextAt(1);
    assertEquals(sequenceManager.notifySuccess(one), SuccessResult.VALID);
    OTransactionIdPromise two = sequenceManager.nextAt(1);
    assertEquals(sequenceManager.notifySuccess(two), SuccessResult.VALID);
    OTransactionIdPromise three = sequenceManager.nextAt(1);
    assertEquals(sequenceManager.notifySuccess(three), SuccessResult.VALID);

    OTransactionSequenceManager sequenceManagerRecv =
        new OTransactionSequenceManager(new ONodeId("two"), 1000);
    assertEquals(sequenceManagerRecv.validate(one), VALID);
    assertEquals(sequenceManagerRecv.validate(three), MISSING_PREVIOUS);

    assertEquals(sequenceManagerRecv.notifySuccess(one), SuccessResult.VALID);
    // This may fail in some cases as early detection
    assertEquals(sequenceManagerRecv.notifySuccess(three), SuccessResult.MISSING_PREVIOUS);

    OTransactionSequenceStatus status = sequenceManager.currentStatus();

    // this will for sure contain two, it may even cantain three
    List<OTransactionId> list = sequenceManagerRecv.checkSelfStatus(status);
    assertNotNull(list);
    assertTrue(
        list.contains(new OTransactionId(two.getId().getPosition(), two.getId().getSequence())));
    assertTrue(
        list.contains(
            new OTransactionId(three.getId().getPosition(), three.getId().getSequence())));
  }

  @Test
  public void sequenceAlreadyPresentSameSpot() {
    OTransactionSequenceManager sequenceManager =
        new OTransactionSequenceManager(new ONodeId("one"), 1000);
    OTransactionIdPromise one = sequenceManager.nextAt(1);
    assertEquals(sequenceManager.notifySuccess(one), SuccessResult.VALID);
    OTransactionIdPromise two = sequenceManager.nextAt(1);
    assertEquals(sequenceManager.notifySuccess(two), SuccessResult.VALID);
    OTransactionIdPromise three = sequenceManager.nextAt(1);
    assertEquals(sequenceManager.notifySuccess(three), SuccessResult.VALID);

    OTransactionSequenceManager sequenceManagerRecv =
        new OTransactionSequenceManager(new ONodeId("two"), 1000);
    assertEquals(sequenceManagerRecv.validate(one), VALID);
    assertEquals(sequenceManagerRecv.validate(three), MISSING_PREVIOUS);

    assertEquals(sequenceManagerRecv.notifySuccess(one), SuccessResult.VALID);
    // This may fail in some cases as early detection
    assertEquals(sequenceManagerRecv.notifySuccess(three), SuccessResult.MISSING_PREVIOUS);

    OTransactionSequenceStatus status = sequenceManager.currentStatus();

    // this will for sure contain two, it may even cantain three
    List<OTransactionId> list = sequenceManagerRecv.checkSelfStatus(status);
    assertNotNull(list);
    assertTrue(
        list.contains(new OTransactionId(two.getId().getPosition(), two.getId().getSequence())));
    assertTrue(
        list.contains(
            new OTransactionId(three.getId().getPosition(), three.getId().getSequence())));
  }

  @Test
  public void sequenceValidSameSpotSameNode() {
    OTransactionSequenceManager sequenceManager =
        new OTransactionSequenceManager(new ONodeId("one"), 1000);
    OTransactionIdPromise one = sequenceManager.nextAt(1);
    assertEquals(
        sequenceManager.validate(
            new OTransactionIdPromise(
                new ONodeId("one"),
                new OTransactionId(one.getId().getPosition(), one.getId().getSequence()))),
        VALID);
    assertEquals(sequenceManager.notifySuccess(one), SuccessResult.VALID);
  }

  @Test
  public void sequenceMissingSameSpotValidation() {
    OTransactionSequenceManager sequenceManager =
        new OTransactionSequenceManager(new ONodeId("one"), 1000);
    OTransactionIdPromise one = sequenceManager.nextAt(1);
    assertEquals(sequenceManager.notifySuccess(one), SuccessResult.VALID);
    OTransactionIdPromise two = sequenceManager.nextAt(1);
    assertEquals(sequenceManager.notifySuccess(two), SuccessResult.VALID);
    OTransactionIdPromise three = sequenceManager.nextAt(1);
    assertEquals(sequenceManager.notifySuccess(three), SuccessResult.VALID);

    OTransactionSequenceManager sequenceManagerRecv =
        new OTransactionSequenceManager(new ONodeId("two"), 1000);
    assertEquals(sequenceManagerRecv.validate(one), VALID);
    assertEquals(sequenceManagerRecv.notifySuccess(one), SuccessResult.VALID);
    assertEquals(sequenceManagerRecv.validate(three), MISSING_PREVIOUS);
  }

  @Test
  public void sequenceMissingSameSpotValidationBack() {
    OTransactionSequenceManager sequenceManager =
        new OTransactionSequenceManager(new ONodeId("one"), 1000);
    OTransactionIdPromise one = sequenceManager.nextAt(1);
    assertEquals(sequenceManager.notifySuccess(one), SuccessResult.VALID);
    OTransactionIdPromise two = sequenceManager.nextAt(1);
    assertEquals(sequenceManager.notifySuccess(two), SuccessResult.VALID);

    OTransactionSequenceManager sequenceManagerOther =
        new OTransactionSequenceManager(new ONodeId("one"), 1000);
    OTransactionIdPromise otherOne = sequenceManagerOther.nextAt(1);
    assertEquals(sequenceManager.notifySuccess(one), SuccessResult.ALREADY_PRESENT);

    OTransactionSequenceManager sequenceManagerRecv =
        new OTransactionSequenceManager(new ONodeId("two"), 1000);
    assertEquals(sequenceManagerRecv.validate(one), VALID);
    assertEquals(sequenceManagerRecv.notifySuccess(one), SuccessResult.VALID);
    assertEquals(sequenceManagerRecv.validate(two), VALID);
    assertEquals(sequenceManagerRecv.notifySuccess(two), SuccessResult.VALID);

    assertEquals(sequenceManagerRecv.validate(otherOne), ALREADY_PRESENT);
  }

  @Test
  public void sequenceMissingSameSpotMissing() {
    OTransactionSequenceManager sequenceManager =
        new OTransactionSequenceManager(new ONodeId("one"), 1000);
    OTransactionIdPromise one = sequenceManager.nextAt(1);
    assertEquals(sequenceManager.notifySuccess(one), SuccessResult.VALID);
    OTransactionIdPromise two = sequenceManager.nextAt(1);
    assertEquals(sequenceManager.notifySuccess(two), SuccessResult.VALID);
    OTransactionIdPromise three = sequenceManager.nextAt(1);
    assertEquals(sequenceManager.notifySuccess(three), SuccessResult.VALID);

    OTransactionSequenceManager sequenceManagerRecv =
        new OTransactionSequenceManager(new ONodeId("two"), 1000);
    assertEquals(sequenceManagerRecv.validate(one), VALID);
    assertEquals(sequenceManagerRecv.notifySuccess(one), SuccessResult.VALID);
    assertEquals(sequenceManagerRecv.validate(two), VALID);
    // This may fail in some cases as early detection
    assertEquals(sequenceManagerRecv.notifySuccess(three), SuccessResult.MISSING_PREVIOUS);

    OTransactionSequenceStatus status = sequenceManager.currentStatus();

    // this will for sure contain two, it may even cantain three
    List<OTransactionId> list = sequenceManagerRecv.checkSelfStatus(status);
    assertNotNull(list);
    // assertTrue(list.contains(two));
    assertTrue(
        list.contains(
            new OTransactionId(three.getId().getPosition(), three.getId().getSequence())));
  }

  @Test
  public void simpleStoreRestore() throws IOException {
    OTransactionSequenceManager sequenceManager =
        new OTransactionSequenceManager(new ONodeId("one"), 1000);
    OTransactionIdPromise one = sequenceManager.next().get();
    OTransactionIdPromise two = sequenceManager.next().get();
    assertEquals(sequenceManager.notifySuccess(one), SuccessResult.VALID);
    assertEquals(sequenceManager.notifySuccess(two), SuccessResult.VALID);
    byte[] bytes = sequenceManager.currentStatus().store();
    OTransactionSequenceManager readSequenceManager =
        new OTransactionSequenceManager(new ONodeId("two"), 1000);
    readSequenceManager.fill(OTransactionSequenceStatus.read(bytes));

    assertEquals(sequenceManager.currentStatus(), readSequenceManager.currentStatus());
  }

  @Test
  public void testAllBusy() {
    OTransactionSequenceManager sequenceManager =
        new OTransactionSequenceManager(new ONodeId("one"), 1000);
    for (int i = 0; i < 1000; i++) {
      sequenceManager.nextAt(i);
    }
    assertFalse(sequenceManager.next().isPresent());
  }

  @Test
  public void testNotificationFailure() {
    OTransactionSequenceManager sequenceManager =
        new OTransactionSequenceManager(new ONodeId("one"), 1000);
    OTransactionIdPromise one = sequenceManager.nextAt(1);
    assertEquals(sequenceManager.notifySuccess(one), SuccessResult.VALID);
    OTransactionIdPromise two = sequenceManager.nextAt(1);
    assertEquals(sequenceManager.notifySuccess(two), SuccessResult.VALID);
    OTransactionIdPromise three = sequenceManager.nextAt(1);
    assertEquals(sequenceManager.notifySuccess(three), SuccessResult.VALID);

    OTransactionSequenceManager sequenceManagerOther =
        new OTransactionSequenceManager(new ONodeId("two"), 1000);
    assertEquals(sequenceManagerOther.validate(one), VALID);
    assertEquals(sequenceManagerOther.notifySuccess(one), SuccessResult.VALID);
    assertEquals(sequenceManagerOther.validate(two), VALID);
    assertEquals(sequenceManagerOther.notifySuccess(two), SuccessResult.VALID);

    OTransactionIdPromise otherThree = sequenceManagerOther.nextAt(1);

    OTransactionSequenceManager sequenceManagerRecv =
        new OTransactionSequenceManager(new ONodeId("three"), 1000);
    assertEquals(sequenceManagerRecv.validate(one), VALID);
    assertEquals(sequenceManagerRecv.notifySuccess(one), SuccessResult.VALID);
    assertEquals(sequenceManagerRecv.validate(two), VALID);
    assertEquals(sequenceManagerRecv.notifySuccess(two), SuccessResult.VALID);
    assertEquals(sequenceManagerRecv.validate(three), VALID);
    assertEquals(sequenceManagerRecv.validate(otherThree), ALREADY_PROMISED);
    assertFalse(sequenceManagerRecv.notifyFailure(otherThree));
    assertTrue(sequenceManagerRecv.notifyFailure(three));
    assertEquals(sequenceManagerRecv.validate(otherThree), VALID);
  }

  @Test
  public void testRetrySequence() {
    var sequenceManager = new OTransactionSequenceManager(new ONodeId("one"), 1000);
    var sequence = sequenceManager.next().get();

    var sequenceManagerOther = new OTransactionSequenceManager(new ONodeId("two"), 1000);
    assertEquals(sequenceManagerOther.validate(sequence), VALID);
    var retrySequence = sequence.retrySequence(new ONodeId("two"));

    assertEquals(sequenceManager.validate(retrySequence), VALID);
    assertEquals(sequenceManagerOther.validate(retrySequence), VALID);
    assertEquals(sequenceManager.notifySuccess(retrySequence), SuccessResult.VALID);
    assertEquals(sequenceManagerOther.notifySuccess(retrySequence), SuccessResult.VALID);
    assertEquals(sequenceManager.validate(sequence), ALREADY_PRESENT);
    assertEquals(sequenceManagerOther.validate(sequence), ALREADY_PRESENT);
  }

  @Test
  public void testRetryWithFailSequence() {
    var sequenceManager = new OTransactionSequenceManager(new ONodeId("one"), 1000);
    var sequence = sequenceManager.next().get();

    var sequenceManagerOther = new OTransactionSequenceManager(new ONodeId("two"), 1000);
    assertEquals(sequenceManagerOther.validate(sequence), VALID);
    var retrySequence = sequence.retrySequence(new ONodeId("two"));

    assertEquals(sequenceManager.validate(retrySequence), VALID);
    assertEquals(sequenceManagerOther.validate(retrySequence), VALID);
    assertTrue(sequenceManager.notifyFailure(retrySequence));
    assertTrue(sequenceManagerOther.notifyFailure(retrySequence));
    // After failure should re-accept the first sequence
    assertEquals(sequenceManager.validate(sequence), VALID);
    assertEquals(sequenceManagerOther.validate(sequence), VALID);
  }
}

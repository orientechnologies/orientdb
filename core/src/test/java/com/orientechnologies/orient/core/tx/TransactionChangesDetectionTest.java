package com.orientechnologies.orient.core.tx;

import static org.junit.Assert.*;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Created by tglman on 02/01/17. */
public class TransactionChangesDetectionTest {
  private OrientDB factory;
  private ODatabaseDocument database;

  @Before
  public void before() {
    factory =
        OCreateDatabaseUtil.createDatabase(
            TransactionChangesDetectionTest.class.getSimpleName(),
            "embedded:",
            OCreateDatabaseUtil.TYPE_MEMORY);
    database =
        factory.open(
            TransactionChangesDetectionTest.class.getSimpleName(),
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    database.createClass("test");
  }

  @After
  public void after() {
    database.close();
    factory.drop(TransactionChangesDetectionTest.class.getSimpleName());
    factory.close();
  }

  @Test
  public void testTransactionChangeTrackingCompleted() {
    database.begin();
    final OTransactionOptimistic currentTx = (OTransactionOptimistic) database.getTransaction();
    database.save(new ODocument("test"));
    assertTrue(currentTx.isChanged());
    assertFalse(currentTx.isAlreadyCleared());
    assertTrue(currentTx.isUsingLog());
    assertEquals(1, currentTx.getEntryCount());
    assertEquals(OTransaction.TXSTATUS.BEGUN, currentTx.getStatus());

    currentTx.resetChangesTracking();
    database.save(new ODocument("test"));
    assertTrue(currentTx.isChanged());
    assertTrue(currentTx.isAlreadyCleared());
    assertEquals(2, currentTx.getEntryCount());
    assertEquals(OTransaction.TXSTATUS.BEGUN, currentTx.getStatus());
    database.commit();
    assertEquals(OTransaction.TXSTATUS.COMPLETED, currentTx.getStatus());
  }

  @Test
  public void testTransactionChangeTrackingRolledBack() {
    database.begin();
    final OTransactionOptimistic currentTx = (OTransactionOptimistic) database.getTransaction();
    database.save(new ODocument("test"));
    assertTrue(currentTx.isChanged());
    assertFalse(currentTx.isAlreadyCleared());
    assertEquals(1, currentTx.getEntryCount());
    assertEquals(OTransaction.TXSTATUS.BEGUN, currentTx.getStatus());
    database.rollback();
    assertEquals(OTransaction.TXSTATUS.ROLLED_BACK, currentTx.getStatus());
  }

  @Test
  public void testTransactionChangeTrackingAfterRollback() {
    database.begin();
    final OTransactionOptimistic initialTx = (OTransactionOptimistic) database.getTransaction();
    database.save(new ODocument("test"));
    assertEquals(1, initialTx.getTxStartCounter());
    database.rollback();
    assertEquals(OTransaction.TXSTATUS.ROLLED_BACK, initialTx.getStatus());
    assertEquals(0, initialTx.getEntryCount());

    database.begin();
    assertTrue(database.getTransaction() instanceof OTransactionOptimistic);
    final OTransactionOptimistic currentTx = (OTransactionOptimistic) database.getTransaction();
    assertEquals(1, currentTx.getTxStartCounter());
    database.save(new ODocument("test"));
    assertTrue(currentTx.isChanged());
    assertFalse(currentTx.isAlreadyCleared());
    assertEquals(1, currentTx.getEntryCount());
    assertEquals(OTransaction.TXSTATUS.BEGUN, currentTx.getStatus());
  }

  @Test
  public void testTransactionTxStartCounterCommits() {
    database.begin();
    final OTransactionOptimistic currentTx = (OTransactionOptimistic) database.getTransaction();
    database.save(new ODocument("test"));
    assertEquals(1, currentTx.getTxStartCounter());
    assertEquals(1, currentTx.getEntryCount());

    database.begin();
    assertEquals(2, currentTx.getTxStartCounter());
    database.commit();
    assertEquals(1, currentTx.getTxStartCounter());
    database.save(new ODocument("test"));
    database.commit();
    assertEquals(0, currentTx.getTxStartCounter());
  }

  @Test(expected = ORollbackException.class)
  public void testTransactionRollbackCommit() {
    database.begin();
    final OTransactionOptimistic currentTx = (OTransactionOptimistic) database.getTransaction();
    assertEquals(1, currentTx.getTxStartCounter());
    database.begin();
    assertEquals(2, currentTx.getTxStartCounter());
    database.rollback();
    assertEquals(1, currentTx.getTxStartCounter());
    database.commit();
    fail("Should throw an 'ORollbackException'.");
  }

  @Test
  public void testTransactionTwoStartedThreeCompleted() {
    database.begin();
    final OTransactionOptimistic currentTx = (OTransactionOptimistic) database.getTransaction();
    assertEquals(1, currentTx.getTxStartCounter());
    database.begin();
    assertEquals(2, currentTx.getTxStartCounter());
    database.commit();
    assertEquals(1, currentTx.getTxStartCounter());
    database.commit();
    assertEquals(0, currentTx.getTxStartCounter());
    database.commit();
    assertFalse(currentTx.isActive());
  }

  @Test
  public void testTransactionCommitForce() {
    database.begin();
    final OTransactionOptimistic currentTx = (OTransactionOptimistic) database.getTransaction();
    assertEquals(1, currentTx.getTxStartCounter());
    database.begin();
    assertEquals(2, currentTx.getTxStartCounter());
    database.commit(true);
    assertEquals(0, currentTx.getTxStartCounter());
    database.commit();
    assertFalse(currentTx.isActive());
  }
}

package com.orientechnologies.orient.core.tx;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
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
    factory = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    factory.create(TransactionChangesDetectionTest.class.getSimpleName(), ODatabaseType.MEMORY);
    database =
        factory.open(TransactionChangesDetectionTest.class.getSimpleName(), "admin", "admin");
    database.createClass("test");
  }

  @After
  public void after() {
    database.close();
    factory.drop(TransactionChangesDetectionTest.class.getSimpleName());
    factory.close();
  }

  @Test
  public void testTransactionChangeTracking() {
    database.begin();
    final OTransactionOptimistic currentTx = (OTransactionOptimistic) database.getTransaction();
    database.save(new ODocument("test"));
    assertTrue(currentTx.isChanged());
    assertFalse(currentTx.isAlreadyCleared());
    assertTrue(currentTx.isUsingLog());

    currentTx.resetChangesTracking();
    database.save(new ODocument("test"));
    assertTrue(currentTx.isChanged());
    assertTrue(currentTx.isAlreadyCleared());
  }
}

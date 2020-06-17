package com.orientechnologies.orient.core.sql.executor;

import static org.assertj.core.api.Assertions.assertThat;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com) */
public class OCreateSequenceStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass
  public static void beforeClass() {
    db = new ODatabaseDocumentTx("memory:OCreateSequenceStatementExecutionTest");
    db.create();
  }

  @AfterClass
  public static void afterClass() {
    db.close();
  }

  @Test
  public void testSimple() {
    db.command("CREATE SEQUENCE Sequence1 TYPE ORDERED");

    OResultSet results = db.query("select sequence('Sequence1').next() as val");
    Assert.assertTrue(results.hasNext());
    OResult result = results.next();
    assertThat((Long) result.getProperty("val")).isEqualTo(1L);
    Assert.assertFalse(results.hasNext());
    results.close();

    results = db.query("select sequence('Sequence1').next() as val");
    Assert.assertTrue(results.hasNext());
    result = results.next();
    assertThat((Long) result.getProperty("val")).isEqualTo(2L);
    Assert.assertFalse(results.hasNext());
    results.close();

    results = db.query("select sequence('Sequence1').next() as val");
    Assert.assertTrue(results.hasNext());
    result = results.next();
    assertThat((Long) result.getProperty("val")).isEqualTo(3L);
    Assert.assertFalse(results.hasNext());
    results.close();
  }

  @Test
  public void testIncrement() {
    db.command("CREATE SEQUENCE SequenceIncrement TYPE ORDERED INCREMENT 3");

    OResultSet results = db.query("select sequence('SequenceIncrement').next() as val");
    Assert.assertTrue(results.hasNext());
    OResult result = results.next();
    assertThat((Long) result.getProperty("val")).isEqualTo(3L);
    Assert.assertFalse(results.hasNext());
    results.close();

    results = db.query("select sequence('SequenceIncrement').next() as val");
    Assert.assertTrue(results.hasNext());
    result = results.next();
    assertThat((Long) result.getProperty("val")).isEqualTo(6L);
    Assert.assertFalse(results.hasNext());
    results.close();

    results = db.query("select sequence('SequenceIncrement').next() as val");
    Assert.assertTrue(results.hasNext());
    result = results.next();
    assertThat((Long) result.getProperty("val")).isEqualTo(9L);
    Assert.assertFalse(results.hasNext());
    results.close();
  }

  @Test
  public void testStart() {
    db.command("CREATE SEQUENCE SequenceStart TYPE ORDERED START 3");

    OResultSet results = db.query("select sequence('SequenceStart').next() as val");
    Assert.assertTrue(results.hasNext());
    OResult result = results.next();
    assertThat((Long) result.getProperty("val")).isEqualTo(4L);
    Assert.assertFalse(results.hasNext());
    results.close();

    results = db.query("select sequence('SequenceStart').next() as val");
    Assert.assertTrue(results.hasNext());
    result = results.next();
    assertThat((Long) result.getProperty("val")).isEqualTo(5L);
    Assert.assertFalse(results.hasNext());
    results.close();

    results = db.query("select sequence('SequenceStart').next() as val");
    Assert.assertTrue(results.hasNext());
    result = results.next();
    assertThat((Long) result.getProperty("val")).isEqualTo(6L);
    Assert.assertFalse(results.hasNext());
    results.close();
  }

  @Test
  public void testStartIncrement() {
    db.command("CREATE SEQUENCE SequenceStartIncrement TYPE ORDERED START 3 INCREMENT 10");

    OResultSet results = db.query("select sequence('SequenceStartIncrement').next() as val");
    Assert.assertTrue(results.hasNext());
    OResult result = results.next();
    assertThat((Long) result.getProperty("val")).isEqualTo(13L);
    Assert.assertFalse(results.hasNext());
    results.close();

    results = db.query("select sequence('SequenceStartIncrement').next() as val");
    Assert.assertTrue(results.hasNext());
    result = results.next();
    assertThat((Long) result.getProperty("val")).isEqualTo(23L);
    Assert.assertFalse(results.hasNext());
    results.close();

    results = db.query("select sequence('SequenceStartIncrement').next() as val");
    Assert.assertTrue(results.hasNext());
    result = results.next();
    assertThat((Long) result.getProperty("val")).isEqualTo(33L);
    Assert.assertFalse(results.hasNext());
    results.close();
  }

  @Test
  public void testCreateSequenceIfNotExists() {
    db.command("CREATE SEQUENCE SequenceIfNotExists if not exists TYPE ORDERED").close();

    OResultSet result =
        db.command("CREATE SEQUENCE SequenceIfNotExists if not exists TYPE ORDERED");

    Assert.assertFalse(result.hasNext());
    result.close();
  }
}

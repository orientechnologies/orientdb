package com.orientechnologies.orient.core.sql.functions.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class OCustomSQLFunctionsTest {

  private static ODatabaseDocumentTx db;

  @BeforeClass
  public static void beforeClass() {
    db = new ODatabaseDocumentTx("memory:" + OCustomSQLFunctionsTest.class.getSimpleName());
    db.create();
  }

  @AfterClass
  public static void afterClass() {
    db.close();
  }

  @Test
  public void testRandom() {
    List<ODocument> result =
        db.query(new OSQLSynchQuery<ODocument>("select math_random() as random"));
    assertTrue((Double) result.get(0).field("random") > 0);
  }

  @Test
  public void testLog10() {
    List<ODocument> result =
        db.query(new OSQLSynchQuery<ODocument>("select math_log10(10000) as log10"));
    assertEquals((Double) result.get(0).field("log10"), 4.0, 0.0001);
  }

  @Test
  public void testAbsInt() {
    List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>("select math_abs(-5) as abs"));
    assertTrue((Integer) result.get(0).field("abs") == 5);
  }

  @Test
  public void testAbsDouble() {
    List<ODocument> result =
        db.query(new OSQLSynchQuery<ODocument>("select math_abs(-5.0d) as abs"));
    assertTrue((Double) result.get(0).field("abs") == 5.0);
  }

  @Test
  public void testAbsFloat() {
    List<ODocument> result =
        db.query(new OSQLSynchQuery<ODocument>("select math_abs(-5.0f) as abs"));
    assertTrue((Float) result.get(0).field("abs") == 5.0);
  }

  @Test(expected = OQueryParsingException.class)
  public void testNonExistingFunction() {
    db.query(new OSQLSynchQuery<ODocument>("select math_min('boom', 'boom') as boom"));
  }
}

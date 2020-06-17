package com.orientechnologies.orient.core.sql.functions.stat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionDecimal;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.math.BigDecimal;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class OSQLFunctionDecimalTest {

  private OSQLFunctionDecimal function;

  @Before
  public void setup() {
    function = new OSQLFunctionDecimal();
  }

  @Test
  public void testEmpty() {
    Object result = function.getResult();
    assertNull(result);
  }

  @Test
  public void testFromInteger() {
    function.execute(null, null, null, new Object[] {12}, null);
    Object result = function.getResult();
    assertEquals(result, new BigDecimal(12));
  }

  @Test
  public void testFromLong() {
    function.execute(null, null, null, new Object[] {1287623847384l}, null);
    Object result = function.getResult();
    assertEquals(result, new BigDecimal(1287623847384l));
  }

  @Test
  public void testFromString() {
    String initial = "12324124321234543256758654.76543212345676543254356765434567654";
    function.execute(null, null, null, new Object[] {initial}, null);
    Object result = function.getResult();
    assertEquals(result, new BigDecimal(initial));
  }

  public void testFromQuery() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:testDecimalFunction");
    db.create();
    String initial = "12324124321234543256758654.76543212345676543254356765434567654";
    List<ODocument> result =
        db.query(new OSQLSynchQuery<ODocument>("select decimal('" + initial + "')"));
    ODocument r = result.get(0);
    assertEquals(result.size(), 1);
    assertEquals(r.field("decimal"), new BigDecimal(initial));
    db.close();
  }
}

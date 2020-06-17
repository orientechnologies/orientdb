package com.orientechnologies.orient.core.sql.functions.math;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the absolute value function. The key is that the mathematical abs function is correctly
 * applied and that values retain their types.
 *
 * @author Michael MacFadden
 */
public class OSQLFunctionAbsoluteValueTest {

  private OSQLFunctionAbsoluteValue function;

  @Before
  public void setup() {
    function = new OSQLFunctionAbsoluteValue();
  }

  @Test
  public void testEmpty() {
    Object result = function.getResult();
    assertNull(result);
  }

  @Test
  public void testNull() {
    function.execute(null, null, null, new Object[] {null}, null);
    Object result = function.getResult();
    assertNull(result);
  }

  @Test
  public void testPositiveInteger() {
    function.execute(null, null, null, new Object[] {10}, null);
    Object result = function.getResult();
    assertTrue(result instanceof Integer);
    assertEquals(result, 10);
  }

  @Test
  public void testNegativeInteger() {
    function.execute(null, null, null, new Object[] {-10}, null);
    Object result = function.getResult();
    assertTrue(result instanceof Integer);
    assertEquals(result, 10);
  }

  @Test
  public void testPositiveLong() {
    function.execute(null, null, null, new Object[] {10L}, null);
    Object result = function.getResult();
    assertTrue(result instanceof Long);
    assertEquals(result, 10L);
  }

  @Test
  public void testNegativeLong() {
    function.execute(null, null, null, new Object[] {-10L}, null);
    Object result = function.getResult();
    assertTrue(result instanceof Long);
    assertEquals(result, 10L);
  }

  @Test
  public void testPositiveShort() {
    function.execute(null, null, null, new Object[] {(short) 10}, null);
    Object result = function.getResult();
    assertTrue(result instanceof Short);
    assertEquals(result, (short) 10);
  }

  @Test
  public void testNegativeShort() {
    function.execute(null, null, null, new Object[] {(short) -10}, null);
    Object result = function.getResult();
    assertTrue(result instanceof Short);
    assertEquals(result, (short) 10);
  }

  @Test
  public void testPositiveDouble() {
    function.execute(null, null, null, new Object[] {10.5D}, null);
    Object result = function.getResult();
    assertTrue(result instanceof Double);
    assertEquals(result, 10.5D);
  }

  @Test
  public void testNegativeDouble() {
    function.execute(null, null, null, new Object[] {-10.5D}, null);
    Object result = function.getResult();
    assertTrue(result instanceof Double);
    assertEquals(result, 10.5D);
  }

  @Test
  public void testPositiveFloat() {
    function.execute(null, null, null, new Object[] {10.5F}, null);
    Object result = function.getResult();
    assertTrue(result instanceof Float);
    assertEquals(result, 10.5F);
  }

  @Test
  public void testNegativeFloat() {
    function.execute(null, null, null, new Object[] {-10.5F}, null);
    Object result = function.getResult();
    assertTrue(result instanceof Float);
    assertEquals(result, 10.5F);
  }

  @Test
  public void testPositiveBigDecimal() {
    function.execute(null, null, null, new Object[] {new BigDecimal(10.5D)}, null);
    Object result = function.getResult();
    assertTrue(result instanceof BigDecimal);
    assertEquals(result, new BigDecimal(10.5D));
  }

  @Test
  public void testNegativeBigDecimal() {
    function.execute(null, null, null, new Object[] {new BigDecimal(-10.5D)}, null);
    Object result = function.getResult();
    assertTrue(result instanceof BigDecimal);
    assertEquals(result, new BigDecimal(10.5D));
  }

  @Test
  public void testPositiveBigInteger() {
    function.execute(null, null, null, new Object[] {new BigInteger("10")}, null);
    Object result = function.getResult();
    assertTrue(result instanceof BigInteger);
    assertEquals(result, new BigInteger("10"));
  }

  @Test
  public void testNegativeBigInteger() {
    function.execute(null, null, null, new Object[] {new BigInteger("-10")}, null);
    Object result = function.getResult();
    assertTrue(result instanceof BigInteger);
    assertEquals(result, new BigInteger("10"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNonNumber() {
    function.execute(null, null, null, new Object[] {"abc"}, null);
  }

  @Test
  public void testFromQuery() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:testAbsFunction");
    db.create();
    List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>("select abs(-45.4)"));
    ODocument r = result.get(0);
    assertEquals(result.size(), 1);
    assertThat(r.<Double>field("abs")).isEqualTo(45.4D);
    db.close();
  }
}

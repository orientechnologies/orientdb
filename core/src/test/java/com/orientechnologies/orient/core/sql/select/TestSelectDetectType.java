package com.orientechnologies.orient.core.sql.select;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Test;

public class TestSelectDetectType extends BaseMemoryDatabase {

  @Test
  public void testFloatDetection() {
    OResultSet res = db.query("select ty.type() as ty from ( select 1.021484375 as ty)");
    assertEquals(res.next().getProperty("ty"), "FLOAT");
    res = db.query("select ty.type() as ty from ( select " + Float.MAX_VALUE + "0101 as ty)");
    assertEquals(res.next().getProperty("ty"), "DOUBLE");
  }
}

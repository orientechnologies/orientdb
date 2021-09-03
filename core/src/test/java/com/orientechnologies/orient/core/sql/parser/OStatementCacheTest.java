package com.orientechnologies.orient.core.sql.parser;

import org.junit.Assert;
import org.junit.Test;

public class OStatementCacheTest {

  @Test
  public void testInIsNotAReservedWord() {
    OStatementCache cache = new OStatementCache(2);
    cache.get("select from foo");
    cache.get("select from bar");
    cache.get("select from baz");

    Assert.assertTrue(cache.contains("select from bar"));
    Assert.assertTrue(cache.contains("select from baz"));
    Assert.assertFalse(cache.contains("select from foo"));

    cache.get("select from bar");
    cache.get("select from foo");

    Assert.assertTrue(cache.contains("select from bar"));
    Assert.assertTrue(cache.contains("select from foo"));
    Assert.assertFalse(cache.contains("select from baz"));
  }
}

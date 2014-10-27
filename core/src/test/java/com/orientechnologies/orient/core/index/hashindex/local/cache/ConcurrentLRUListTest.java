package com.orientechnologies.orient.core.index.hashindex.local.cache;

import org.testng.annotations.BeforeMethod;

public class ConcurrentLRUListTest extends LRUListTest {
  @BeforeMethod
  public void setUp() throws Exception {
    lruList = new ConcurrentLRUList();
  }
}

package com.orientechnologies.orient.core.index.hashindex.local.cache;

import com.orientechnologies.orient.core.storage.cache.local.twoq.ConcurrentLRUList;
import org.junit.Before;

public class ConcurrentLRUListTest extends AbstractLRUListTestTemplate {
  @Before
  public void setUp() throws Exception {
    lruList = new ConcurrentLRUList();
  }
}

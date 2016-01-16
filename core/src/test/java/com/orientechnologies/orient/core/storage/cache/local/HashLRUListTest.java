package com.orientechnologies.orient.core.storage.cache.local;

import com.orientechnologies.orient.core.index.hashindex.local.cache.LRUListTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
@Test
public class HashLRUListTest extends LRUListTest {
  @BeforeMethod
  public void setUp() throws Exception {
    lruList = new HashLRUList();
  }
}

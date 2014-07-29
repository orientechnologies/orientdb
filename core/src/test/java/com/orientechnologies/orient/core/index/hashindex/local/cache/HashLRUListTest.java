package com.orientechnologies.orient.core.index.hashindex.local.cache;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
@Test
public class HashLRUListTest extends LRUListTest {
  @BeforeMethod
  public void setUp() throws Exception {
    lruList = new HashLRUList();
  }
}

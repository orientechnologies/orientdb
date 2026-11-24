package com.orientechnologies.orient.core.index;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Collections;
import org.junit.Test;

public class OIndexManagerRemoteTest {

  @Test
  public void testPushDoesNotClearIndexCacheDuringNestedReload() {
    final TestIndexManager indexManager = new TestIndexManager();
    indexManager.cacheIndexesForClass("X");

    indexManager.beginOperation();
    try {
      indexManager.beginOperation();
      indexManager.endOperation();

      indexManager.update(new ODocument());
      assertTrue(indexManager.hasCachedIndexesForClass("X"));
    } finally {
      indexManager.endOperation();
    }

    indexManager.update(new ODocument());
    assertFalse(indexManager.hasCachedIndexesForClass("X"));
  }

  private static final class TestIndexManager extends OIndexManagerRemote {
    private TestIndexManager() {
      super(null);
    }

    private void beginOperation() {
      acquireExclusiveLock();
    }

    private void endOperation() {
      releaseExclusiveLock();
    }

    private void cacheIndexesForClass(final String className) {
      classPropertyIndex.put(className, Collections.emptyMap());
    }

    private boolean hasCachedIndexesForClass(final String className) {
      return classPropertyIndex.containsKey(className);
    }
  }
}

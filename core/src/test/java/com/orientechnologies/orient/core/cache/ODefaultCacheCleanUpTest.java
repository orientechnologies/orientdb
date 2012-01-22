package com.orientechnologies.orient.core.cache;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public class ODefaultCacheCleanUpTest {
  public void removesGivenAmountOfRecords() {
    //Given filled cache backend
    ODefaultCache.OLinkedHashMapCache sut = filledCacheBackend();
    int originalSize = sut.size();

    //When asked to remove eldest items
    int amount = 10;
    sut.removeEldest(amount);

    //Then new cache size should be of original size minus amount of deleted items
    assertEquals(sut.size(), originalSize - amount);
  }

  public void doesNotTakeDirtyRecordsIntoAccountWhenSkips() {
    //Given filled cache backend
    //With some dirty records in it
    ODefaultCache.OLinkedHashMapCache sut = filledCacheBackendWithSomeDirtyRecords();
    int originalSize = sut.size();

    //When asked to remove eldest items
    int amount = 10;
    sut.removeEldest(amount);

    //Then removes less then asked
    assertTrue(amount > originalSize - sut.size());
  }

  public void clearsWholeCacheIfMemoryCriticallyLow() {
    //Given running filled cache
    ODefaultCache sut = runningFilledCache();

    //When watchdog listener invoked with critically low memory
    int freeMemoryPercentageBelowCriticalPoint = 8;
    sut.lowMemoryListener.memoryUsageLow(1, freeMemoryPercentageBelowCriticalPoint);

    //Then whole cache cleared
    assertEquals(sut.size(), 0, "Cache has entries in it yet");
  }

  public void removesPartOfEntriesInCaseOfLowMemory() {
    //Given running filled cache
    ODefaultCache sut = runningFilledCache();
    int originalSize = sut.size();

    //When watchdog listener invoked with critically low memory
    int freeMemoryPercentageBelowCriticalPoint = 20;
    sut.lowMemoryListener.memoryUsageLow(1, freeMemoryPercentageBelowCriticalPoint);

    //Then whole cache cleared
    assertTrue(sut.size() < originalSize, "Cache was not cleaned");
    assertTrue(sut.size() > 0, "Cache was cleared wholly");
  }


  private ODefaultCache.OLinkedHashMapCache filledCacheBackend() {
    ODefaultCache.OLinkedHashMapCache cache = new ODefaultCache.OLinkedHashMapCache(100, 0.75f, 100);
    for (int i = 100; i > 0; i--) {
      ODocument entry = new ODocument(new ORecordId(i, i));
      cache.put(entry.getIdentity(), entry);
    }
    return cache;
  }

  private ODefaultCache.OLinkedHashMapCache filledCacheBackendWithSomeDirtyRecords() {
    ODefaultCache.OLinkedHashMapCache cache = filledCacheBackend();
    int i = 0;
    for (Map.Entry<ORID, ORecordInternal<?>> entry : cache.entrySet()) {
      if (i++ % 3 == 0) entry.getValue().setDirty();
    }
    return cache;
  }

  private ODefaultCache runningFilledCache() {
    ODefaultCache cache = new ODefaultCache(100);
    cache.startup();
    for (int i = 100; i > 0; i--) cache.put(new ODocument(new ORecordId(i, i)));
    return cache;
  }
}

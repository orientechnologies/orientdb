package com.orientechnologies.orient.core.storage.cache.chm;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.ODirectMemoryAllocator;
import com.orientechnologies.common.directmemory.ODirectMemoryAllocator.Intention;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class WTinyLFUPolicyTest {
  @Test
  public void testEden() {
    ODirectMemoryAllocator memoryAllocator = new ODirectMemoryAllocator();
    OByteBufferPool pool = new OByteBufferPool(1, memoryAllocator, 0);

    ConcurrentHashMap<PageKey, OCacheEntry> data = new ConcurrentHashMap<>();
    Admittor admittor = mock(Admittor.class);

    AtomicInteger cacheSize = new AtomicInteger();

    WTinyLFUPolicy wTinyLFU = new WTinyLFUPolicy(data, admittor, cacheSize);
    wTinyLFU.setMaxSize(15);

    final OCacheEntry[] cacheEntries = new OCacheEntry[3];
    final OCachePointer[] cachePointers = new OCachePointer[3];

    generateEntries(cacheEntries, cachePointers, pool);

    cacheSize.incrementAndGet();
    wTinyLFU.onAdd(cacheEntries[0]);

    cacheSize.incrementAndGet();
    wTinyLFU.onAdd(cacheEntries[1]);

    cacheSize.incrementAndGet();
    wTinyLFU.onAdd(cacheEntries[2]);

    Assert.assertEquals(3, cacheSize.get());
    Assert.assertEquals(15, wTinyLFU.getMaxSize());

    {
      final Iterator<OCacheEntry> probationIterator = wTinyLFU.probation();
      Assert.assertFalse(probationIterator.hasNext());
    }

    {
      final Iterator<OCacheEntry> protectionIterator = wTinyLFU.protection();
      Assert.assertFalse(protectionIterator.hasNext());
    }

    Assert.assertArrayEquals(
        new OCacheEntry[] {cacheEntries[2], cacheEntries[1], cacheEntries[0]},
        toArray(wTinyLFU.eden()));

    wTinyLFU.onAccess(cacheEntries[1]);

    {
      final Iterator<OCacheEntry> probationIterator = wTinyLFU.probation();
      Assert.assertFalse(probationIterator.hasNext());
    }

    {
      final Iterator<OCacheEntry> protectionIterator = wTinyLFU.protection();
      Assert.assertFalse(protectionIterator.hasNext());
    }

    Assert.assertArrayEquals(
        new OCacheEntry[] {cacheEntries[1], cacheEntries[2], cacheEntries[0]},
        toArray(wTinyLFU.eden()));

    wTinyLFU.onAccess(cacheEntries[1]);

    {
      final Iterator<OCacheEntry> probationIterator = wTinyLFU.probation();
      Assert.assertFalse(probationIterator.hasNext());
    }

    {
      final Iterator<OCacheEntry> protectionIterator = wTinyLFU.protection();
      Assert.assertFalse(protectionIterator.hasNext());
    }
    Assert.assertArrayEquals(
        new OCacheEntry[] {cacheEntries[1], cacheEntries[2], cacheEntries[0]},
        toArray(wTinyLFU.eden()));

    wTinyLFU.onAccess(cacheEntries[0]);

    {
      final Iterator<OCacheEntry> probationIterator = wTinyLFU.probation();
      Assert.assertFalse(probationIterator.hasNext());
    }

    {
      final Iterator<OCacheEntry> protectionIterator = wTinyLFU.protection();
      Assert.assertFalse(protectionIterator.hasNext());
    }

    Assert.assertArrayEquals(
        new OCacheEntry[] {cacheEntries[0], cacheEntries[1], cacheEntries[2]},
        toArray(wTinyLFU.eden()));

    Assert.assertEquals(3, cacheSize.get());

    clearPointers(wTinyLFU);
    Mockito.<Object>reset(admittor);
  }

  @Test
  public void testGoLastToProtection() {
    ODirectMemoryAllocator memoryAllocator = new ODirectMemoryAllocator();
    OByteBufferPool pool = new OByteBufferPool(1, memoryAllocator, 0);

    ConcurrentHashMap<PageKey, OCacheEntry> data = new ConcurrentHashMap<>();
    Admittor admittor = mock(Admittor.class);

    AtomicInteger cacheSize = new AtomicInteger();
    WTinyLFUPolicy wTinyLFU = new WTinyLFUPolicy(data, admittor, cacheSize);
    wTinyLFU.setMaxSize(15);

    OCacheEntry[] cacheEntries = new OCacheEntry[4];
    OCachePointer[] cachePointers = new OCachePointer[4];

    generateEntries(cacheEntries, cachePointers, pool);

    cacheSize.incrementAndGet();
    wTinyLFU.onAdd(cacheEntries[0]);

    cacheSize.incrementAndGet();
    wTinyLFU.onAdd(cacheEntries[1]);

    cacheSize.incrementAndGet();
    wTinyLFU.onAdd(cacheEntries[2]);

    cacheSize.incrementAndGet();
    wTinyLFU.onAdd(cacheEntries[3]);

    Assert.assertArrayEquals(new OCacheEntry[] {cacheEntries[0]}, toArray(wTinyLFU.probation()));
    Assert.assertFalse(wTinyLFU.protection().hasNext());
    Assert.assertArrayEquals(
        new OCacheEntry[] {cacheEntries[3], cacheEntries[2], cacheEntries[1]},
        toArray(wTinyLFU.eden()));

    Assert.assertEquals(4, cacheSize.get());

    clearPointers(wTinyLFU);
    Mockito.<Object>reset(admittor);
  }

  @Test
  public void testProbationIsFull() {
    ODirectMemoryAllocator memoryAllocator = new ODirectMemoryAllocator();
    OByteBufferPool pool = new OByteBufferPool(1, memoryAllocator, 0);

    ConcurrentHashMap<PageKey, OCacheEntry> data = new ConcurrentHashMap<>();
    Admittor admittor = mock(Admittor.class);

    AtomicInteger cacheSize = new AtomicInteger();
    WTinyLFUPolicy wTinyLFU = new WTinyLFUPolicy(data, admittor, cacheSize);
    wTinyLFU.setMaxSize(15);

    OCacheEntry[] cacheEntries = new OCacheEntry[15];
    OCachePointer[] cachePointers = new OCachePointer[15];

    generateEntries(cacheEntries, cachePointers, pool);

    for (int i = 0; i < 15; i++) {
      cacheSize.incrementAndGet();
      wTinyLFU.onAdd(cacheEntries[i]);
    }

    Iterator<OCacheEntry> probationIterator = wTinyLFU.probation();

    for (int i = 11; i >= 0; i--) {
      final OCacheEntry cacheEntry = probationIterator.next();
      Assert.assertSame(cacheEntries[i], cacheEntry);
    }

    Assert.assertFalse(wTinyLFU.protection().hasNext());

    Iterator<OCacheEntry> edenIterator = wTinyLFU.eden();
    for (int i = 14; i >= 12; i--) {
      final OCacheEntry cacheEntry = edenIterator.next();
      Assert.assertSame(cacheEntries[i], cacheEntry);
    }

    clearPointers(wTinyLFU);
    Mockito.<Object>reset(admittor);
  }

  @Test
  public void testProbationIsOverflownNoProtectionOne() {
    ODirectMemoryAllocator memoryAllocator = new ODirectMemoryAllocator();
    OByteBufferPool pool = new OByteBufferPool(1, memoryAllocator, 0);

    ConcurrentHashMap<PageKey, OCacheEntry> data = new ConcurrentHashMap<>();
    Admittor admittor = mock(Admittor.class);

    AtomicInteger cacheSize = new AtomicInteger();
    WTinyLFUPolicy wTinyLFU = new WTinyLFUPolicy(data, admittor, cacheSize);
    wTinyLFU.setMaxSize(15);

    OCacheEntry[] cacheEntries = new OCacheEntry[16];
    OCachePointer[] cachePointers = new OCachePointer[16];

    generateEntries(cacheEntries, cachePointers, pool);

    when(admittor.frequency(PageKey.hashCode(1, 0))).thenReturn(0);
    when(admittor.frequency(PageKey.hashCode(1, 12))).thenReturn(1);

    for (int i = 0; i < 16; i++) {
      cacheSize.incrementAndGet();
      data.put(new PageKey(1, i), cacheEntries[i]);
      wTinyLFU.onAdd(cacheEntries[i]);
    }

    Assert.assertEquals(15, cacheSize.get());
    wTinyLFU.assertSize();
    wTinyLFU.assertConsistency();

    Assert.assertEquals(15, memoryAllocator.getMemoryConsumption());

    Assert.assertFalse(wTinyLFU.protection().hasNext());
    Iterator<OCacheEntry> edenIterator = wTinyLFU.eden();

    for (int i = 15; i >= 13; i--) {
      final OCacheEntry cacheEntry = edenIterator.next();
      Assert.assertSame(cacheEntry, cacheEntries[i]);
    }

    Iterator<OCacheEntry> probationIterator = wTinyLFU.probation();
    for (int i = 12; i >= 1; i--) {
      final OCacheEntry cacheEntry = probationIterator.next();
      Assert.assertSame(cacheEntry, cacheEntries[i]);
    }

    clearPointers(wTinyLFU);
  }

  @Test
  public void testProbationIsOverflownNoProtectionTwo() {
    ODirectMemoryAllocator memoryAllocator = new ODirectMemoryAllocator();
    OByteBufferPool pool = new OByteBufferPool(1, memoryAllocator, 0);

    ConcurrentHashMap<PageKey, OCacheEntry> data = new ConcurrentHashMap<>();
    Admittor admittor = mock(Admittor.class);

    AtomicInteger cacheSize = new AtomicInteger();
    WTinyLFUPolicy wTinyLFU = new WTinyLFUPolicy(data, admittor, cacheSize);
    wTinyLFU.setMaxSize(15);

    OCacheEntry[] cacheEntries = new OCacheEntry[16];
    OCachePointer[] cachePointers = new OCachePointer[16];

    generateEntries(cacheEntries, cachePointers, pool);

    when(admittor.frequency(PageKey.hashCode(1, 0))).thenReturn(1);
    when(admittor.frequency(PageKey.hashCode(1, 12))).thenReturn(0);

    for (int i = 0; i < 16; i++) {
      cacheSize.incrementAndGet();
      data.put(new PageKey(1, i), cacheEntries[i]);
      wTinyLFU.onAdd(cacheEntries[i]);
    }

    Assert.assertEquals(15, cacheSize.get());
    wTinyLFU.assertSize();
    wTinyLFU.assertConsistency();

    Assert.assertEquals(15, memoryAllocator.getMemoryConsumption());

    Assert.assertFalse(wTinyLFU.protection().hasNext());
    Iterator<OCacheEntry> edenIterator = wTinyLFU.eden();

    for (int i = 15; i >= 13; i--) {
      final OCacheEntry cacheEntry = edenIterator.next();
      Assert.assertSame(cacheEntry, cacheEntries[i]);
    }

    Iterator<OCacheEntry> probationIterator = wTinyLFU.probation();
    for (int i = 11; i >= 0; i--) {
      final OCacheEntry cacheEntry = probationIterator.next();
      Assert.assertSame(cacheEntry, cacheEntries[i]);
    }

    clearPointers(wTinyLFU);
  }

  @Test
  public void testProbationIsOverflownNoProtectionVictimOneIsAcquired() {
    ODirectMemoryAllocator memoryAllocator = new ODirectMemoryAllocator();
    OByteBufferPool pool = new OByteBufferPool(1, memoryAllocator, 0);

    ConcurrentHashMap<PageKey, OCacheEntry> data = new ConcurrentHashMap<>();
    Admittor admittor = mock(Admittor.class);

    AtomicInteger cacheSize = new AtomicInteger();
    WTinyLFUPolicy wTinyLFU = new WTinyLFUPolicy(data, admittor, cacheSize);
    wTinyLFU.setMaxSize(15);

    OCacheEntry[] cacheEntries = new OCacheEntry[16];
    OCachePointer[] cachePointers = new OCachePointer[16];

    generateEntries(cacheEntries, cachePointers, pool);

    when(admittor.frequency(PageKey.hashCode(1, 0))).thenReturn(0);
    when(admittor.frequency(PageKey.hashCode(1, 12))).thenReturn(1);

    when(admittor.frequency(PageKey.hashCode(1, 1))).thenReturn(0);
    when(admittor.frequency(PageKey.hashCode(1, 13))).thenReturn(1);

    cacheEntries[0].acquireEntry();

    for (int i = 0; i < 16; i++) {
      cacheSize.incrementAndGet();
      data.put(new PageKey(1, i), cacheEntries[i]);
      wTinyLFU.onAdd(cacheEntries[i]);
    }

    cacheEntries[0].releaseEntry();

    Assert.assertEquals(15, cacheSize.get());
    wTinyLFU.assertSize();
    wTinyLFU.assertConsistency();

    Assert.assertEquals(15, memoryAllocator.getMemoryConsumption());

    Assert.assertFalse(wTinyLFU.protection().hasNext());
    Iterator<OCacheEntry> edenIterator = wTinyLFU.eden();

    Assert.assertSame(cacheEntries[0], edenIterator.next());

    for (int i = 15; i >= 14; i--) {
      final OCacheEntry cacheEntry = edenIterator.next();
      Assert.assertSame(cacheEntry, cacheEntries[i]);
    }

    Iterator<OCacheEntry> probationIterator = wTinyLFU.probation();
    for (int i = 13; i >= 2; i--) {
      final OCacheEntry cacheEntry = probationIterator.next();
      Assert.assertSame(cacheEntry, cacheEntries[i]);
    }

    clearPointers(wTinyLFU);
  }

  @Test
  public void testProbationIsOverflownNoProtectionTwoIsAcquired() {
    ODirectMemoryAllocator memoryAllocator = new ODirectMemoryAllocator();
    OByteBufferPool pool = new OByteBufferPool(1, memoryAllocator, 0);

    ConcurrentHashMap<PageKey, OCacheEntry> data = new ConcurrentHashMap<>();
    Admittor admittor = mock(Admittor.class);

    AtomicInteger cacheSize = new AtomicInteger();
    WTinyLFUPolicy wTinyLFU = new WTinyLFUPolicy(data, admittor, cacheSize);
    wTinyLFU.setMaxSize(15);

    OCacheEntry[] cacheEntries = new OCacheEntry[16];
    OCachePointer[] cachePointers = new OCachePointer[16];

    generateEntries(cacheEntries, cachePointers, pool);

    when(admittor.frequency(PageKey.hashCode(1, 0))).thenReturn(1);
    when(admittor.frequency(PageKey.hashCode(1, 12))).thenReturn(0);

    when(admittor.frequency(PageKey.hashCode(1, 0))).thenReturn(1);
    when(admittor.frequency(PageKey.hashCode(1, 13))).thenReturn(0);

    cacheEntries[12].acquireEntry();

    for (int i = 0; i < 16; i++) {
      cacheSize.incrementAndGet();
      data.put(new PageKey(1, i), cacheEntries[i]);
      wTinyLFU.onAdd(cacheEntries[i]);
    }

    cacheEntries[12].releaseEntry();

    Assert.assertEquals(15, cacheSize.get());
    wTinyLFU.assertSize();
    wTinyLFU.assertConsistency();

    Assert.assertEquals(15, memoryAllocator.getMemoryConsumption());

    Assert.assertFalse(wTinyLFU.protection().hasNext());
    Iterator<OCacheEntry> edenIterator = wTinyLFU.eden();

    Assert.assertSame(cacheEntries[12], edenIterator.next());

    for (int i = 15; i >= 14; i--) {
      final OCacheEntry cacheEntry = edenIterator.next();
      Assert.assertSame(cacheEntry, cacheEntries[i]);
    }

    Iterator<OCacheEntry> probationIterator = wTinyLFU.probation();
    for (int i = 11; i >= 0; i--) {
      final OCacheEntry cacheEntry = probationIterator.next();
      Assert.assertSame(cacheEntry, cacheEntries[i]);
    }

    clearPointers(wTinyLFU);
  }

  @Test
  public void testProtectedOverflow() {
    ODirectMemoryAllocator memoryAllocator = new ODirectMemoryAllocator();
    OByteBufferPool pool = new OByteBufferPool(1, memoryAllocator, 0);

    ConcurrentHashMap<PageKey, OCacheEntry> data = new ConcurrentHashMap<>();
    Admittor admittor = mock(Admittor.class);

    AtomicInteger cacheSize = new AtomicInteger();
    WTinyLFUPolicy wTinyLFU = new WTinyLFUPolicy(data, admittor, cacheSize);
    wTinyLFU.setMaxSize(15);

    OCacheEntry[] cacheEntries = new OCacheEntry[15];
    OCachePointer[] cachePointers = new OCachePointer[15];

    generateEntries(cacheEntries, cachePointers, pool);

    for (int i = 0; i < 15; i++) {
      cacheSize.incrementAndGet();
      data.put(new PageKey(1, i), cacheEntries[i]);
      wTinyLFU.onAdd(cacheEntries[i]);
    }

    for (int i = 0; i < 11; i++) {
      wTinyLFU.onAccess(cacheEntries[i]);
    }

    final Iterator<OCacheEntry> edenIterator = wTinyLFU.eden();
    for (int i = 14; i >= 13; i--) {
      final OCacheEntry cacheEntry = edenIterator.next();
      Assert.assertSame(cacheEntries[i], cacheEntry);
    }

    final Iterator<OCacheEntry> protectionIterator = wTinyLFU.protection();
    for (int i = 10; i >= 1; i--) {
      final OCacheEntry cacheEntry = protectionIterator.next();
      Assert.assertSame(cacheEntry, cacheEntries[i]);
    }

    wTinyLFU.assertSize();
    wTinyLFU.assertConsistency();

    clearPointers(wTinyLFU);
  }

  @Test
  public void testProtection() {
    ODirectMemoryAllocator memoryAllocator = new ODirectMemoryAllocator();
    OByteBufferPool pool = new OByteBufferPool(1, memoryAllocator, 0);

    ConcurrentHashMap<PageKey, OCacheEntry> data = new ConcurrentHashMap<>();
    Admittor admittor = mock(Admittor.class);

    AtomicInteger cacheSize = new AtomicInteger();
    WTinyLFUPolicy wTinyLFU = new WTinyLFUPolicy(data, admittor, cacheSize);
    wTinyLFU.setMaxSize(15);

    OCacheEntry[] cacheEntries = new OCacheEntry[6];
    OCachePointer[] cachePointers = new OCachePointer[6];

    generateEntries(cacheEntries, cachePointers, pool);

    for (int i = 0; i < 6; i++) {
      cacheSize.incrementAndGet();
      data.put(new PageKey(1, i), cacheEntries[i]);
      wTinyLFU.onAdd(cacheEntries[i]);
    }

    for (int i = 0; i < 3; i++) {
      wTinyLFU.onAccess(cacheEntries[i]);
    }

    final Iterator<OCacheEntry> edenIterator = wTinyLFU.eden();
    for (int i = 5; i >= 3; i--) {
      final OCacheEntry cacheEntry = edenIterator.next();
      Assert.assertSame(cacheEntries[i], cacheEntry);
    }

    Iterator<OCacheEntry> probationIterator = wTinyLFU.probation();
    Assert.assertFalse(probationIterator.hasNext());

    Iterator<OCacheEntry> protectionIterator = wTinyLFU.protection();
    for (int i = 2; i >= 0; i--) {
      final OCacheEntry cacheEntry = protectionIterator.next();
      Assert.assertSame(cacheEntries[i], cacheEntry);
    }

    Assert.assertEquals(6, cacheSize.get());

    wTinyLFU.onAccess(cacheEntries[1]);

    protectionIterator = wTinyLFU.protection();

    Assert.assertSame(cacheEntries[1], protectionIterator.next());
    Assert.assertSame(cacheEntries[2], protectionIterator.next());
    Assert.assertSame(cacheEntries[0], protectionIterator.next());

    Assert.assertFalse(protectionIterator.hasNext());

    wTinyLFU.assertSize();
    wTinyLFU.assertConsistency();

    clearPointers(wTinyLFU);
  }

  @Test
  public void testRemovedEden() {
    ODirectMemoryAllocator memoryAllocator = new ODirectMemoryAllocator();
    OByteBufferPool pool = new OByteBufferPool(1, memoryAllocator, 0);

    ConcurrentHashMap<PageKey, OCacheEntry> data = new ConcurrentHashMap<>();
    Admittor admittor = mock(Admittor.class);

    AtomicInteger cacheSize = new AtomicInteger();
    WTinyLFUPolicy wTinyLFU = new WTinyLFUPolicy(data, admittor, cacheSize);
    wTinyLFU.setMaxSize(15);

    OCacheEntry[] cacheEntries = new OCacheEntry[1];
    OCachePointer[] cachePointers = new OCachePointer[1];

    generateEntries(cacheEntries, cachePointers, pool);

    cacheSize.incrementAndGet();
    data.put(new PageKey(1, 0), cacheEntries[0]);
    wTinyLFU.onAdd(cacheEntries[0]);

    cacheEntries[0].freeze();
    data.remove(new PageKey(1, 0), cacheEntries[0]);
    cacheSize.decrementAndGet();
    wTinyLFU.onRemove(cacheEntries[0]);

    Assert.assertTrue(cacheEntries[0].isDead());
    Assert.assertFalse(wTinyLFU.eden().hasNext());
    Assert.assertFalse(wTinyLFU.probation().hasNext());
    Assert.assertFalse(wTinyLFU.protection().hasNext());

    wTinyLFU.assertConsistency();
    wTinyLFU.assertSize();

    Assert.assertEquals(0, cacheSize.get());
    Assert.assertEquals(0, memoryAllocator.getMemoryConsumption());
    clearPointers(wTinyLFU);
  }

  @Test
  public void testRemoveProbation() {
    ODirectMemoryAllocator memoryAllocator = new ODirectMemoryAllocator();
    OByteBufferPool pool = new OByteBufferPool(1, memoryAllocator, 0);

    ConcurrentHashMap<PageKey, OCacheEntry> data = new ConcurrentHashMap<>();
    Admittor admittor = mock(Admittor.class);

    AtomicInteger cacheSize = new AtomicInteger();
    WTinyLFUPolicy wTinyLFU = new WTinyLFUPolicy(data, admittor, cacheSize);
    wTinyLFU.setMaxSize(15);

    OCacheEntry[] cacheEntries = new OCacheEntry[6];
    OCachePointer[] cachePointers = new OCachePointer[6];

    generateEntries(cacheEntries, cachePointers, pool);

    for (int i = 0; i < 6; i++) {
      cacheSize.incrementAndGet();
      data.put(new PageKey(1, i), cacheEntries[i]);
      wTinyLFU.onAdd(cacheEntries[i]);
    }

    cacheEntries[0].freeze();
    data.remove(new PageKey(1, 0), cacheEntries[0]);
    cacheSize.decrementAndGet();
    wTinyLFU.onRemove(cacheEntries[0]);

    Assert.assertEquals(5, cacheSize.get());
    Assert.assertFalse(wTinyLFU.protection().hasNext());

    Iterator<OCacheEntry> edenIterator = wTinyLFU.eden();
    for (int i = 5; i >= 3; i--) {
      final OCacheEntry cacheEntry = edenIterator.next();
      Assert.assertSame(cacheEntries[i], cacheEntry);
    }

    Iterator<OCacheEntry> probationIterator = wTinyLFU.probation();
    for (int i = 2; i >= 1; i--) {
      final OCacheEntry cacheEntry = probationIterator.next();
      Assert.assertSame(cacheEntries[i], cacheEntry);
    }

    Assert.assertTrue(cacheEntries[0].isDead());

    wTinyLFU.assertSize();
    wTinyLFU.assertConsistency();

    Assert.assertEquals(5, memoryAllocator.getMemoryConsumption());
    clearPointers(wTinyLFU);
  }

  @Test
  public void testRemoveProtection() {
    ODirectMemoryAllocator memoryAllocator = new ODirectMemoryAllocator();
    OByteBufferPool pool = new OByteBufferPool(1, memoryAllocator, 0);

    ConcurrentHashMap<PageKey, OCacheEntry> data = new ConcurrentHashMap<>();
    Admittor admittor = mock(Admittor.class);

    AtomicInteger cacheSize = new AtomicInteger();
    WTinyLFUPolicy wTinyLFU = new WTinyLFUPolicy(data, admittor, cacheSize);
    wTinyLFU.setMaxSize(15);

    OCacheEntry[] cacheEntries = new OCacheEntry[6];
    OCachePointer[] cachePointers = new OCachePointer[6];

    generateEntries(cacheEntries, cachePointers, pool);

    for (int i = 0; i < 6; i++) {
      cacheSize.incrementAndGet();
      data.put(new PageKey(1, i), cacheEntries[i]);
      wTinyLFU.onAdd(cacheEntries[i]);
    }

    for (int i = 0; i < 3; i++) {
      wTinyLFU.onAccess(cacheEntries[i]);
    }

    cacheEntries[0].freeze();
    data.remove(new PageKey(1, 0), cacheEntries[0]);
    cacheSize.decrementAndGet();
    wTinyLFU.onRemove(cacheEntries[0]);

    Assert.assertEquals(5, cacheSize.get());
    Assert.assertTrue(cacheEntries[0].isDead());

    Assert.assertFalse(wTinyLFU.probation().hasNext());

    Iterator<OCacheEntry> edenIterator = wTinyLFU.eden();
    for (int i = 5; i >= 3; i--) {
      final OCacheEntry cacheEntry = edenIterator.next();
      Assert.assertSame(cacheEntries[i], cacheEntry);
    }

    Iterator<OCacheEntry> protactionIterator = wTinyLFU.protection();
    for (int i = 2; i >= 1; i--) {
      final OCacheEntry cacheEntry = protactionIterator.next();
      Assert.assertSame(cacheEntries[i], cacheEntry);
    }

    wTinyLFU.assertSize();
    wTinyLFU.assertConsistency();

    Assert.assertEquals(5, memoryAllocator.getMemoryConsumption());
    clearPointers(wTinyLFU);
  }

  private static OCacheEntry[] toArray(Iterator<OCacheEntry> iterator) {
    final List<OCacheEntry> entries = new ArrayList<>();
    while (iterator.hasNext()) {
      final OCacheEntry cacheEntry = iterator.next();
      entries.add(cacheEntry);
    }

    return entries.toArray(new OCacheEntry[0]);
  }

  private static void generateEntries(
      OCacheEntry[] cacheEntries, OCachePointer[] cachePointers, OByteBufferPool pool) {
    for (int i = 0; i < cacheEntries.length; i++) {
      final OCachePointer cachePointer =
          new OCachePointer(pool.acquireDirect(true, Intention.TEST), pool, 1, i);
      final OCacheEntry cacheEntry = new OCacheEntryImpl(1, i, cachePointer, false, null);

      cachePointer.incrementReadersReferrer();
      cacheEntries[i] = cacheEntry;
      cachePointers[i] = cachePointer;
    }
  }

  private static void clearPointers(WTinyLFUPolicy policy) {
    clearQueue(policy.eden());
    clearQueue(policy.probation());
    clearQueue(policy.protection());
  }

  private static void clearQueue(final Iterator<OCacheEntry> iterator) {
    while (iterator.hasNext()) {
      final OCacheEntry cacheEntry = iterator.next();
      cacheEntry.getCachePointer().decrementReadersReferrer();
    }
  }
}

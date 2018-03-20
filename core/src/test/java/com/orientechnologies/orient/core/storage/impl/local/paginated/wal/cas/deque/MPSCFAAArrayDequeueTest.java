package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.deque;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

public class MPSCFAAArrayDequeueTest {
  @Test
  public void testSingleItem() {
    MPSCFAAArrayDequeue<EmptyRecord> dequeue = new MPSCFAAArrayDequeue<>();
    EmptyRecord record = new EmptyRecord();

    dequeue.offer(record);
    Cursor<EmptyRecord> cursor = dequeue.peekFirst();

    Assert.assertNotNull(cursor);
    Assert.assertEquals(record, cursor.getItem());

    cursor = MPSCFAAArrayDequeue.next(cursor);
    Assert.assertNull(cursor);

    cursor = dequeue.peekLast();
    Assert.assertNotNull(cursor);

    Assert.assertEquals(record, cursor.getItem());
    cursor = MPSCFAAArrayDequeue.prev(cursor);

    Assert.assertNull(cursor);

    Assert.assertEquals(record, dequeue.peek());
    Assert.assertEquals(record, dequeue.peek());

    Assert.assertEquals(record, dequeue.poll());
    Assert.assertNull(dequeue.poll());
    Assert.assertNull(dequeue.poll());

    Assert.assertNull(dequeue.peek());
    Assert.assertNull(dequeue.peekFirst());
    Assert.assertNull(dequeue.peekLast());
  }

  @Test
  public void testFewItems() {
    MPSCFAAArrayDequeue<EmptyRecord> dequeue = new MPSCFAAArrayDequeue<>();
    List<EmptyRecord> records = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      final EmptyRecord emptyRecord = new EmptyRecord();
      records.add(emptyRecord);
      dequeue.offer(emptyRecord);

      assertForward(dequeue, records);
      assertBackward(dequeue, records);

      Assert.assertEquals(records.get(0), dequeue.peek());
    }

    assertForward(dequeue, records);
    assertBackward(dequeue, records);

    for (int i = 0; i < 5; i++) {
      EmptyRecord record = records.remove(0);
      EmptyRecord removedRecord = dequeue.poll();
      Assert.assertEquals(record, removedRecord);

      assertForward(dequeue, records);
      assertBackward(dequeue, records);

      if (records.isEmpty()) {
        Assert.assertNull(dequeue.peek());
      } else {
        Assert.assertEquals(records.get(0), dequeue.peek());
      }
    }

    assertForward(dequeue, records);
    assertBackward(dequeue, records);
    Assert.assertNull(dequeue.peek());
  }

  @Test
  public void testSeveralPages() {
    MPSCFAAArrayDequeue<EmptyRecord> dequeue = new MPSCFAAArrayDequeue<>();
    List<EmptyRecord> records = new ArrayList<>();

    final int items = Node.BUFFER_SIZE * 3 + Node.BUFFER_SIZE / 2;

    for (int i = 0; i < items; i++) {
      final EmptyRecord emptyRecord = new EmptyRecord();
      records.add(emptyRecord);
      dequeue.offer(emptyRecord);

      assertForward(dequeue, records);
      assertBackward(dequeue, records);

      Assert.assertEquals(records.get(0), dequeue.peek());
    }

    assertForward(dequeue, records);
    assertBackward(dequeue, records);

    for (int i = 0; i < items; i++) {
      EmptyRecord record = records.remove(0);
      EmptyRecord removedRecord = dequeue.poll();
      Assert.assertEquals(record, removedRecord);

      assertForward(dequeue, records);
      assertBackward(dequeue, records);

      if (records.isEmpty()) {
        Assert.assertNull(dequeue.peek());
      } else {
        Assert.assertEquals(records.get(0), dequeue.peek());
      }
    }

    Assert.assertNull(dequeue.peek());

    assertForward(dequeue, records);
    assertBackward(dequeue, records);
  }

  @Test
  public void testRandomPolling() {
    final int iterations = 1_000_000;

    for (int n = 0; n < iterations; n++) {
      final long seed = System.nanoTime();
      try {
        final Random random = new Random(seed);

        MPSCFAAArrayDequeue<EmptyRecord> dequeue = new MPSCFAAArrayDequeue<>();
        List<EmptyRecord> records = new ArrayList<>();

        final int items = Node.BUFFER_SIZE * 10 + Node.BUFFER_SIZE / 2;
        final double pollShare = random.nextDouble();

        for (int i = 0; i < items; i++) {
          final double action = random.nextDouble();
          if (action < pollShare) {
            final EmptyRecord record = dequeue.poll();

            if (records.isEmpty()) {
              Assert.assertNull(record);
            } else {
              Assert.assertEquals(records.remove(0), record);
            }
          } else {
            final EmptyRecord emptyRecord = new EmptyRecord();
            records.add(emptyRecord);
            dequeue.offer(emptyRecord);

            assertForward(dequeue, records);
            assertBackward(dequeue, records);
          }

          if (records.isEmpty()) {
            Assert.assertNull(dequeue.peek());
          } else {
            Assert.assertEquals(records.get(0), dequeue.peek());
          }
        }

        assertForward(dequeue, records);
        assertBackward(dequeue, records);

        final int recordsCount = records.size();
        for (int i = 0; i < recordsCount; i++) {
          EmptyRecord record = records.remove(0);
          EmptyRecord removedRecord = dequeue.poll();
          Assert.assertEquals(record, removedRecord);

          assertForward(dequeue, records);
          assertBackward(dequeue, records);

          if (records.isEmpty()) {
            Assert.assertNull(dequeue.peek());
          } else {
            Assert.assertEquals(records.get(0), dequeue.peek());
          }
        }

        Assert.assertNull(dequeue.peek());

        assertForward(dequeue, records);
        assertBackward(dequeue, records);

        if (n > 0 && n % 100 == 0) {
          System.out.printf("%d iterations were passed \n", n);
        }
      } catch (Exception | Error e) {
        System.out.println("testRandomPolling seed : " + seed);
        throw e;
      }
    }
  }

  @Test
  public void mtTestForwardSevenThreads() throws Exception {
    final int iterations = 150;

    for (int n = 0; n < iterations; n++) {
      final ExecutorService executor = Executors.newCachedThreadPool();
      final MPSCFAAArrayDequeue<EmptyRecord> dequeue = new MPSCFAAArrayDequeue<>();

      final int enquersCount = 7;
      final int limit = 100_000_000;

      final AtomicBoolean stop = new AtomicBoolean();
      final Future<Void> dequer = executor.submit(new Dequer(dequeue, stop, enquersCount, limit));

      final List<Future<Void>> enquers = new ArrayList<>();

      for (int i = 0; i < enquersCount; i++) {
        enquers.add(executor.submit(new Enquer(limit, i, dequeue, enquersCount)));
      }

      for (Future<Void> enquer : enquers) {
        enquer.get();
      }

      stop.set(true);

      dequer.get();

      executor.shutdown();

      System.out.printf("%d iterations were passed out of %d \n", n, iterations);
    }
  }

  @Test
  public void mtTestForwardTwoThreads() throws Exception {
    final int iterations = 3_000;

    for (int n = 0; n < iterations; n++) {
      final ExecutorService executor = Executors.newCachedThreadPool();
      final MPSCFAAArrayDequeue<EmptyRecord> dequeue = new MPSCFAAArrayDequeue<>();

      final int enquersCount = 1;
      final int limit = 1_000_000_000;

      final AtomicBoolean stop = new AtomicBoolean();
      final Future<Void> dequer = executor.submit(new Dequer(dequeue, stop, enquersCount, limit));

      final List<Future<Void>> enquers = new ArrayList<>();

      for (int i = 0; i < enquersCount; i++) {
        enquers.add(executor.submit(new Enquer(limit, i, dequeue, enquersCount)));
      }

      for (Future<Void> enquer : enquers) {
        enquer.get();
      }

      stop.set(true);

      dequer.get();

      executor.shutdown();

      System.out.printf("%d iterations were passed out of %d \n", n, iterations);
    }
  }

  private static final class Enquer implements Callable<Void> {
    private final int                              limit;
    private final int                              segment;
    private final MPSCFAAArrayDequeue<EmptyRecord> dequeue;
    private final int                              segments;

    Enquer(int limit, int segment, MPSCFAAArrayDequeue<EmptyRecord> dequeue, int segments) {
      this.limit = limit;
      this.segment = segment;
      this.dequeue = dequeue;
      this.segments = segments;
    }

    @Override
    public Void call() {
      try {
        final ThreadLocalRandom random = ThreadLocalRandom.current();

        for (int i = 0; i < limit; i++) {
          final EmptyRecord record = new EmptyRecord();
          record.setLsn(new OLogSequenceNumber(segment, i));

          dequeue.offer(record);

          if (random.nextDouble() <= 0.2) {
            iterateForward(random);
          }
        }
      } catch (Exception | Error e) {
        e.printStackTrace();
        throw e;
      }

      return null;
    }

    private void iterateForward(ThreadLocalRandom random) {
      final int batchSize = random.nextInt(10_000) + 1;
      int counter = 0;

      int[] segmentPositions = new int[segments];
      Arrays.fill(segmentPositions, -1);

      Cursor<EmptyRecord> cursor = dequeue.peekFirst();

      while (cursor != null && counter < batchSize) {
        final OLogSequenceNumber lsn = cursor.item.getLsn();
        final int segment = (int) lsn.getSegment();

        if (segmentPositions[segment] > 0) {
          Assert.assertEquals(segmentPositions[segment] + 1, lsn.getPosition());
        }

        segmentPositions[segment] = (int) lsn.getPosition();

        cursor = MPSCFAAArrayDequeue.next(cursor);
        counter++;
      }
    }
  }

  private static final class Dequer implements Callable<Void> {
    private final MPSCFAAArrayDequeue<EmptyRecord> dequeue;
    private final AtomicBoolean                    stop;
    private final int                              segments;
    private final int                              limit;

    Dequer(MPSCFAAArrayDequeue<EmptyRecord> dequeue, AtomicBoolean stop, int segments, int limit) {
      this.dequeue = dequeue;
      this.stop = stop;
      this.segments = segments;
      this.limit = limit;
    }

    @Override
    public Void call() {
      try {
        int[] segmentPositions = new int[segments];
        Arrays.fill(segmentPositions, -1);

        while (!stop.get()) {
          final EmptyRecord record = dequeue.poll();
          if (record == null) {
            continue;
          }

          final OLogSequenceNumber lsn = record.getLsn();
          final int segment = (int) lsn.getSegment();
          Assert.assertEquals(segmentPositions[segment] + 1, lsn.getPosition());
          segmentPositions[segment]++;
        }

        for (int position : segmentPositions) {
          Assert.assertEquals(limit - 1, position);
        }

      } catch (Exception | Error e) {
        e.printStackTrace();
        throw e;
      }

      return null;
    }
  }

  private void assertBackward(MPSCFAAArrayDequeue<EmptyRecord> dequeue, List<EmptyRecord> records) {
    ListIterator<EmptyRecord> recordIterator = records.listIterator(records.size());
    Cursor<EmptyRecord> recordCursor = dequeue.peekLast();

    while (recordIterator.hasPrevious()) {
      Assert.assertNotNull(recordCursor);
      Assert.assertEquals(recordIterator.previous(), recordCursor.getItem());
      recordCursor = MPSCFAAArrayDequeue.prev(recordCursor);
    }

    Assert.assertNull(recordCursor);
  }

  private void assertForward(MPSCFAAArrayDequeue<EmptyRecord> dequeue, List<EmptyRecord> records) {
    Iterator<EmptyRecord> recordIterator = records.iterator();
    Cursor<EmptyRecord> recordCursor = dequeue.peekFirst();

    while (recordIterator.hasNext()) {
      Assert.assertNotNull(recordCursor);
      Assert.assertEquals(recordIterator.next(), recordCursor.getItem());
      recordCursor = MPSCFAAArrayDequeue.next(recordCursor);
    }

    Assert.assertNull(recordCursor);
  }

  private static final class EmptyRecord implements OWALRecord {
    private OLogSequenceNumber lsn;

    @Override
    public OLogSequenceNumber getLsn() {
      return lsn;
    }

    @Override
    public void setLsn(OLogSequenceNumber lsn) {
      this.lsn = lsn;
    }

    @Override
    public boolean casLSN(OLogSequenceNumber currentLSN, OLogSequenceNumber newLSN) {
      return false;
    }

    @Override
    public void setDistance(int distance) {

    }

    @Override
    public void setDiskSize(int diskSize) {

    }

    @Override
    public int getDistance() {
      return 0;
    }

    @Override
    public int getDiskSize() {
      return 0;
    }
  }
}

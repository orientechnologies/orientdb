package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.deque;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;

public class MPSCFAAArrayDequeueTest {
  @Test
  public void testSingleItem() {
    MPSCFAAArrayDequeue dequeue = new MPSCFAAArrayDequeue();
    EmptyRecord record = new EmptyRecord();

    dequeue.offer(record);
    Cursor cursor = dequeue.peekFirst();

    Assert.assertNotNull(cursor);
    Assert.assertEquals(record, cursor.getRecord());

    cursor = MPSCFAAArrayDequeue.next(cursor);
    Assert.assertNull(cursor);

    cursor = dequeue.peekLast();
    Assert.assertNotNull(cursor);

    Assert.assertEquals(record, cursor.getRecord());
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
    MPSCFAAArrayDequeue dequeue = new MPSCFAAArrayDequeue();
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
      EmptyRecord removedRecord = (EmptyRecord) dequeue.poll();
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
    MPSCFAAArrayDequeue dequeue = new MPSCFAAArrayDequeue();
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
      EmptyRecord removedRecord = (EmptyRecord) dequeue.poll();
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

        MPSCFAAArrayDequeue dequeue = new MPSCFAAArrayDequeue();
        List<EmptyRecord> records = new ArrayList<>();

        final int items = Node.BUFFER_SIZE * 10 + Node.BUFFER_SIZE / 2;
        final double pollShare = random.nextDouble();

        for (int i = 0; i < items; i++) {
          final double action = random.nextDouble();
          if (action < pollShare) {
            final EmptyRecord record = (EmptyRecord) dequeue.poll();

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
          EmptyRecord removedRecord = (EmptyRecord) dequeue.poll();
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

  private void assertBackward(MPSCFAAArrayDequeue dequeue, List<EmptyRecord> records) {
    ListIterator<EmptyRecord> recordIterator = records.listIterator(records.size());
    Cursor recordCursor = dequeue.peekLast();

    while (recordIterator.hasPrevious()) {
      Assert.assertNotNull(recordCursor);
      Assert.assertEquals(recordIterator.previous(), recordCursor.getRecord());
      recordCursor = MPSCFAAArrayDequeue.prev(recordCursor);
    }

    Assert.assertNull(recordCursor);
  }

  private void assertForward(MPSCFAAArrayDequeue dequeue, List<EmptyRecord> records) {
    Iterator<EmptyRecord> recordIterator = records.iterator();
    Cursor recordCursor = dequeue.peekFirst();

    while (recordIterator.hasNext()) {
      Assert.assertNotNull(recordCursor);
      Assert.assertEquals(recordIterator.next(), recordCursor.getRecord());
      recordCursor = MPSCFAAArrayDequeue.next(recordCursor);
    }

    Assert.assertNull(recordCursor);
  }

  private static final class EmptyRecord implements OWALRecord {
    @Override
    public OLogSequenceNumber getLsn() {
      return null;
    }

    @Override
    public void setLsn(OLogSequenceNumber lsn) {

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

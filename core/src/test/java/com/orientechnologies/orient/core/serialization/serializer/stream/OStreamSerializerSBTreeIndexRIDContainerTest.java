package com.orientechnologies.orient.core.serialization.serializer.stream;

import com.orientechnologies.BaseMemoryInternalDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALPageChangesPortion;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OIndexRIDContainer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Test;

public class OStreamSerializerSBTreeIndexRIDContainerTest extends BaseMemoryInternalDatabase {

  private final OStreamSerializerSBTreeIndexRIDContainer streamSerializerSBTreeIndexRIDContainer =
      new OStreamSerializerSBTreeIndexRIDContainer();

  @Test
  public void testSerializeInByteBufferEmbeddedNonDurable() throws IOException {
    final OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) db.getStorage();
    final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();
    atomicOperationsManager.startAtomicOperation(null);
    try {

      final OIndexRIDContainer indexRIDContainer =
          new OIndexRIDContainer("test", false, new AtomicLong(0));

      indexRIDContainer.setTopThreshold(100);
      for (int i = 0; i < 5; i++) {
        indexRIDContainer.add(new ORecordId(1, i));
      }

      Assert.assertTrue(indexRIDContainer.isEmbedded());

      final int len = streamSerializerSBTreeIndexRIDContainer.getObjectSize(indexRIDContainer);
      final int serializationOffset = 5;

      final ByteBuffer buffer = ByteBuffer.allocate(len + serializationOffset);
      buffer.position(serializationOffset);

      streamSerializerSBTreeIndexRIDContainer.serializeInByteBufferObject(
          indexRIDContainer, buffer);

      final int binarySize = buffer.position() - serializationOffset;
      Assert.assertEquals(binarySize, len);

      buffer.position(serializationOffset);
      Assert.assertEquals(
          streamSerializerSBTreeIndexRIDContainer.getObjectSizeInByteBuffer(buffer), len);

      buffer.position(serializationOffset);
      OIndexRIDContainer newRidContainer =
          streamSerializerSBTreeIndexRIDContainer.deserializeFromByteBufferObject(buffer);

      Assert.assertEquals(buffer.position() - serializationOffset, len);
      Assert.assertNotSame(newRidContainer, indexRIDContainer);

      Assert.assertTrue(newRidContainer.isEmbedded());
      Assert.assertFalse(newRidContainer.isDurableNonTxMode());

      final Set<OIdentifiable> storedRids = new HashSet<>(newRidContainer);
      final Set<OIdentifiable> newRids = new HashSet<>(indexRIDContainer);

      Assert.assertEquals(newRids, storedRids);
    } finally {
      atomicOperationsManager.endAtomicOperation(null);
    }
  }

  @Test
  public void testSerializeInByteBufferEmbeddedDurable() throws IOException {
    final OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) db.getStorage();
    final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();
    atomicOperationsManager.startAtomicOperation(null);
    try {

      final OIndexRIDContainer indexRIDContainer =
          new OIndexRIDContainer("test", true, new AtomicLong(0));

      indexRIDContainer.setTopThreshold(100);
      for (int i = 0; i < 5; i++) {
        indexRIDContainer.add(new ORecordId(1, i * 2));
      }

      Assert.assertTrue(indexRIDContainer.isEmbedded());

      final int len = streamSerializerSBTreeIndexRIDContainer.getObjectSize(indexRIDContainer);
      final int serializationOffset = 5;

      final ByteBuffer buffer = ByteBuffer.allocate(len + serializationOffset);
      buffer.position(serializationOffset);

      streamSerializerSBTreeIndexRIDContainer.serializeInByteBufferObject(
          indexRIDContainer, buffer);

      final int binarySize = buffer.position() - serializationOffset;
      Assert.assertEquals(binarySize, len);

      buffer.position(serializationOffset);
      Assert.assertEquals(
          streamSerializerSBTreeIndexRIDContainer.getObjectSizeInByteBuffer(buffer), len);

      buffer.position(serializationOffset);
      OIndexRIDContainer newRidContainer =
          streamSerializerSBTreeIndexRIDContainer.deserializeFromByteBufferObject(buffer);

      Assert.assertEquals(buffer.position() - serializationOffset, len);
      Assert.assertNotSame(newRidContainer, indexRIDContainer);

      Assert.assertTrue(newRidContainer.isEmbedded());
      Assert.assertTrue(newRidContainer.isDurableNonTxMode());

      final Set<OIdentifiable> storedRids = new HashSet<>(newRidContainer);
      final Set<OIdentifiable> newRids = new HashSet<>(indexRIDContainer);

      Assert.assertEquals(newRids, storedRids);
    } finally {
      atomicOperationsManager.endAtomicOperation(null);
    }
  }

  @Test
  public void testSerializeInImmutableByteBufferPositionEmbeddedDurable() throws IOException {
    final OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) db.getStorage();
    final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();
    atomicOperationsManager.startAtomicOperation(null);
    try {

      final OIndexRIDContainer indexRIDContainer =
          new OIndexRIDContainer("test", true, new AtomicLong(0));

      indexRIDContainer.setTopThreshold(100);
      for (int i = 0; i < 5; i++) {
        indexRIDContainer.add(new ORecordId(1, i * 2));
      }

      Assert.assertTrue(indexRIDContainer.isEmbedded());

      final int len = streamSerializerSBTreeIndexRIDContainer.getObjectSize(indexRIDContainer);
      final int serializationOffset = 5;

      final ByteBuffer buffer = ByteBuffer.allocate(len + serializationOffset);
      buffer.position(serializationOffset);

      streamSerializerSBTreeIndexRIDContainer.serializeInByteBufferObject(
          indexRIDContainer, buffer);

      final int binarySize = buffer.position() - serializationOffset;
      Assert.assertEquals(binarySize, len);

      buffer.position(0);
      Assert.assertEquals(
          streamSerializerSBTreeIndexRIDContainer.getObjectSizeInByteBuffer(
              serializationOffset, buffer),
          len);
      Assert.assertEquals(0, buffer.position());

      OIndexRIDContainer newRidContainer =
          streamSerializerSBTreeIndexRIDContainer.deserializeFromByteBufferObject(
              serializationOffset, buffer);
      Assert.assertEquals(0, buffer.position());

      Assert.assertNotSame(newRidContainer, indexRIDContainer);

      Assert.assertTrue(newRidContainer.isEmbedded());
      Assert.assertTrue(newRidContainer.isDurableNonTxMode());

      final Set<OIdentifiable> storedRids = new HashSet<>(newRidContainer);
      final Set<OIdentifiable> newRids = new HashSet<>(indexRIDContainer);

      Assert.assertEquals(newRids, storedRids);
    } finally {
      atomicOperationsManager.endAtomicOperation(null);
    }
  }

  @Test
  public void testSerializeInByteBufferNonEmbeddedDurable() throws IOException {
    final OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) db.getStorage();
    final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();
    atomicOperationsManager.startAtomicOperation(null);
    try {

      final OIndexRIDContainer indexRIDContainer =
          new OIndexRIDContainer("test", true, new AtomicLong(0));

      indexRIDContainer.setTopThreshold(1);
      for (int i = 0; i < 5; i++) {
        indexRIDContainer.add(new ORecordId(1, i * 4));
      }

      Assert.assertFalse(indexRIDContainer.isEmbedded());

      final int len = streamSerializerSBTreeIndexRIDContainer.getObjectSize(indexRIDContainer);
      final int serializationOffset = 5;

      final ByteBuffer buffer = ByteBuffer.allocate(len + serializationOffset);
      buffer.position(serializationOffset);

      streamSerializerSBTreeIndexRIDContainer.serializeInByteBufferObject(
          indexRIDContainer, buffer);

      final int binarySize = buffer.position() - serializationOffset;
      Assert.assertEquals(binarySize, len);

      buffer.position(serializationOffset);
      Assert.assertEquals(
          streamSerializerSBTreeIndexRIDContainer.getObjectSizeInByteBuffer(buffer), len);

      buffer.position(serializationOffset);
      OIndexRIDContainer newRidContainer =
          streamSerializerSBTreeIndexRIDContainer.deserializeFromByteBufferObject(buffer);

      Assert.assertEquals(buffer.position() - serializationOffset, len);
      Assert.assertNotSame(newRidContainer, indexRIDContainer);

      Assert.assertFalse(newRidContainer.isEmbedded());
      Assert.assertTrue(newRidContainer.isDurableNonTxMode());

      final Set<OIdentifiable> storedRids = new HashSet<>(newRidContainer);
      final Set<OIdentifiable> newRids = new HashSet<>(indexRIDContainer);

      Assert.assertEquals(newRids, storedRids);
    } finally {
      atomicOperationsManager.endAtomicOperation(null);
    }
  }

  @Test
  public void testSerializeInImmutableByteBufferPositionNonEmbeddedDurable() throws IOException {
    final OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) db.getStorage();
    final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();
    atomicOperationsManager.startAtomicOperation(null);
    try {

      final OIndexRIDContainer indexRIDContainer =
          new OIndexRIDContainer("test", true, new AtomicLong(0));

      indexRIDContainer.setTopThreshold(1);
      for (int i = 0; i < 5; i++) {
        indexRIDContainer.add(new ORecordId(1, i * 4));
      }

      Assert.assertFalse(indexRIDContainer.isEmbedded());

      final int len = streamSerializerSBTreeIndexRIDContainer.getObjectSize(indexRIDContainer);
      final int serializationOffset = 5;

      final ByteBuffer buffer = ByteBuffer.allocate(len + serializationOffset);
      buffer.position(serializationOffset);

      streamSerializerSBTreeIndexRIDContainer.serializeInByteBufferObject(
          indexRIDContainer, buffer);

      final int binarySize = buffer.position() - serializationOffset;
      Assert.assertEquals(binarySize, len);

      buffer.position(0);
      Assert.assertEquals(
          streamSerializerSBTreeIndexRIDContainer.getObjectSizeInByteBuffer(
              serializationOffset, buffer),
          len);
      Assert.assertEquals(0, buffer.position());

      OIndexRIDContainer newRidContainer =
          streamSerializerSBTreeIndexRIDContainer.deserializeFromByteBufferObject(
              serializationOffset, buffer);
      Assert.assertEquals(0, buffer.position());

      Assert.assertNotSame(newRidContainer, indexRIDContainer);

      Assert.assertFalse(newRidContainer.isEmbedded());
      Assert.assertTrue(newRidContainer.isDurableNonTxMode());

      final Set<OIdentifiable> storedRids = new HashSet<>(newRidContainer);
      final Set<OIdentifiable> newRids = new HashSet<>(indexRIDContainer);

      Assert.assertEquals(newRids, storedRids);
    } finally {
      atomicOperationsManager.endAtomicOperation(null);
    }
  }

  @Test
  public void testSerializeWALChangesEmbeddedNonDurable() throws IOException {
    final OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) db.getStorage();
    final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();
    atomicOperationsManager.startAtomicOperation(null);
    try {
      final OIndexRIDContainer indexRIDContainer =
          new OIndexRIDContainer("test", false, new AtomicLong(0));

      indexRIDContainer.setTopThreshold(100);
      for (int i = 0; i < 5; i++) {
        indexRIDContainer.add(new ORecordId(1, i));
      }

      Assert.assertTrue(indexRIDContainer.isEmbedded());

      final int len = streamSerializerSBTreeIndexRIDContainer.getObjectSize(indexRIDContainer);
      final int serializationOffset = 5;

      final ByteBuffer buffer =
          ByteBuffer.allocateDirect(
                  len + serializationOffset + OWALPageChangesPortion.PORTION_BYTES)
              .order(ByteOrder.nativeOrder());
      final byte[] data = new byte[len];
      streamSerializerSBTreeIndexRIDContainer.serializeNativeObject(indexRIDContainer, data, 0);

      final OWALChanges walChanges = new OWALPageChangesPortion();
      walChanges.setBinaryValue(buffer, data, serializationOffset);

      Assert.assertEquals(
          streamSerializerSBTreeIndexRIDContainer.getObjectSizeInByteBuffer(
              buffer, walChanges, serializationOffset),
          len);
      OIndexRIDContainer newRidContainer =
          streamSerializerSBTreeIndexRIDContainer.deserializeFromByteBufferObject(
              buffer, walChanges, serializationOffset);
      Assert.assertEquals(0, buffer.position());

      Assert.assertNotSame(newRidContainer, indexRIDContainer);

      Assert.assertTrue(newRidContainer.isEmbedded());
      Assert.assertFalse(newRidContainer.isDurableNonTxMode());

      final Set<OIdentifiable> storedRids = new HashSet<>(newRidContainer);
      final Set<OIdentifiable> newRids = new HashSet<>(indexRIDContainer);

      Assert.assertEquals(newRids, storedRids);
    } finally {
      atomicOperationsManager.endAtomicOperation(null);
    }
  }

  @Test
  public void testSerializeWALChangesEmbeddedDurable() throws IOException {
    final OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) db.getStorage();
    final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();
    atomicOperationsManager.startAtomicOperation(null);
    try {

      final OIndexRIDContainer indexRIDContainer =
          new OIndexRIDContainer("test", true, new AtomicLong(0));

      indexRIDContainer.setTopThreshold(100);
      for (int i = 0; i < 5; i++) {
        indexRIDContainer.add(new ORecordId(1, i * 2));
      }

      Assert.assertTrue(indexRIDContainer.isEmbedded());

      final int len = streamSerializerSBTreeIndexRIDContainer.getObjectSize(indexRIDContainer);
      final int serializationOffset = 5;

      final ByteBuffer buffer =
          ByteBuffer.allocateDirect(
                  len + serializationOffset + OWALPageChangesPortion.PORTION_BYTES)
              .order(ByteOrder.nativeOrder());
      final byte[] data = new byte[len];
      streamSerializerSBTreeIndexRIDContainer.serializeNativeObject(indexRIDContainer, data, 0);

      final OWALChanges walChanges = new OWALPageChangesPortion();
      walChanges.setBinaryValue(buffer, data, serializationOffset);

      Assert.assertEquals(
          streamSerializerSBTreeIndexRIDContainer.getObjectSizeInByteBuffer(
              buffer, walChanges, serializationOffset),
          len);
      OIndexRIDContainer newRidContainer =
          streamSerializerSBTreeIndexRIDContainer.deserializeFromByteBufferObject(
              buffer, walChanges, serializationOffset);

      Assert.assertNotSame(newRidContainer, indexRIDContainer);

      Assert.assertTrue(newRidContainer.isEmbedded());
      Assert.assertTrue(newRidContainer.isDurableNonTxMode());

      final Set<OIdentifiable> storedRids = new HashSet<>(newRidContainer);
      final Set<OIdentifiable> newRids = new HashSet<>(indexRIDContainer);

      Assert.assertEquals(newRids, storedRids);
    } finally {
      atomicOperationsManager.endAtomicOperation(null);
    }
  }

  @Test
  public void testSerializeWALChangesNonEmbeddedNonDurable() throws IOException {
    final OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) db.getStorage();
    final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();
    atomicOperationsManager.startAtomicOperation(null);
    try {

      final OIndexRIDContainer indexRIDContainer =
          new OIndexRIDContainer("test", false, new AtomicLong(0));

      indexRIDContainer.setTopThreshold(1);
      for (int i = 0; i < 5; i++) {
        indexRIDContainer.add(new ORecordId(1, i * 3));
      }

      Assert.assertFalse(indexRIDContainer.isEmbedded());

      final int len = streamSerializerSBTreeIndexRIDContainer.getObjectSize(indexRIDContainer);
      final int serializationOffset = 5;

      final ByteBuffer buffer =
          ByteBuffer.allocateDirect(
                  len + serializationOffset + OWALPageChangesPortion.PORTION_BYTES)
              .order(ByteOrder.nativeOrder());
      final byte[] data = new byte[len];
      streamSerializerSBTreeIndexRIDContainer.serializeNativeObject(indexRIDContainer, data, 0);

      final OWALChanges walChanges = new OWALPageChangesPortion();
      walChanges.setBinaryValue(buffer, data, serializationOffset);

      Assert.assertEquals(
          streamSerializerSBTreeIndexRIDContainer.getObjectSizeInByteBuffer(
              buffer, walChanges, serializationOffset),
          len);
      OIndexRIDContainer newRidContainer =
          streamSerializerSBTreeIndexRIDContainer.deserializeFromByteBufferObject(
              buffer, walChanges, serializationOffset);

      Assert.assertNotSame(newRidContainer, indexRIDContainer);

      Assert.assertFalse(newRidContainer.isEmbedded());
      Assert.assertFalse(newRidContainer.isDurableNonTxMode());

      final Set<OIdentifiable> storedRids = new HashSet<>(newRidContainer);
      final Set<OIdentifiable> newRids = new HashSet<>(indexRIDContainer);

      Assert.assertEquals(newRids, storedRids);
    } finally {
      atomicOperationsManager.endAtomicOperation(null);
    }
  }

  @Test
  public void testSerializeWALChangesNonEmbeddedDurable() throws IOException {
    final OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) db.getStorage();
    final OAtomicOperationsManager atomicOperationsManager = storage.getAtomicOperationsManager();
    atomicOperationsManager.startAtomicOperation(null);
    try {

      final OIndexRIDContainer indexRIDContainer =
          new OIndexRIDContainer("test", true, new AtomicLong(0));

      indexRIDContainer.setTopThreshold(1);
      for (int i = 0; i < 5; i++) {
        indexRIDContainer.add(new ORecordId(1, i * 4));
      }

      Assert.assertFalse(indexRIDContainer.isEmbedded());

      final int len = streamSerializerSBTreeIndexRIDContainer.getObjectSize(indexRIDContainer);
      final int serializationOffset = 5;

      final ByteBuffer buffer =
          ByteBuffer.allocateDirect(
                  len + serializationOffset + OWALPageChangesPortion.PORTION_BYTES)
              .order(ByteOrder.nativeOrder());
      final byte[] data = new byte[len];
      streamSerializerSBTreeIndexRIDContainer.serializeNativeObject(indexRIDContainer, data, 0);

      final OWALChanges walChanges = new OWALPageChangesPortion();
      walChanges.setBinaryValue(buffer, data, serializationOffset);

      Assert.assertEquals(
          streamSerializerSBTreeIndexRIDContainer.getObjectSizeInByteBuffer(
              buffer, walChanges, serializationOffset),
          len);
      OIndexRIDContainer newRidContainer =
          streamSerializerSBTreeIndexRIDContainer.deserializeFromByteBufferObject(
              buffer, walChanges, serializationOffset);

      Assert.assertNotSame(newRidContainer, indexRIDContainer);

      Assert.assertFalse(newRidContainer.isEmbedded());
      Assert.assertTrue(newRidContainer.isDurableNonTxMode());

      final Set<OIdentifiable> storedRids = new HashSet<>(newRidContainer);
      final Set<OIdentifiable> newRids = new HashSet<>(indexRIDContainer);

      Assert.assertEquals(newRids, storedRids);
    } finally {
      atomicOperationsManager.endAtomicOperation(null);
    }
  }
}

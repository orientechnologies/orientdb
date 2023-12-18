package com.orientechnologies.orient.core.serialization.serializer.stream;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OIndexRIDContainer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OStreamSerializerSBTreeIndexRIDContainerTest {
  private ODatabaseDocumentTx database;
  private OStreamSerializerSBTreeIndexRIDContainer streamSerializerSBTreeIndexRIDContainer;

  @Before
  public void beforeClass() {
    database = new ODatabaseDocumentTx("memory:" + this.getClass().getSimpleName());
    database.create();
    streamSerializerSBTreeIndexRIDContainer = new OStreamSerializerSBTreeIndexRIDContainer();
  }

  @After
  public void afterClass() {
    database.drop();
  }

  @Test
  public void testSerializeInByteBufferEmbeddedNonDurable() throws IOException {
    final OAbstractPaginatedStorage storage =
        (OAbstractPaginatedStorage) ODatabaseRecordThreadLocal.instance().get().getStorage();
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
    final OAbstractPaginatedStorage storage =
        (OAbstractPaginatedStorage) ODatabaseRecordThreadLocal.instance().get().getStorage();
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
  public void testSerializeInByteBufferNonEmbeddedDurable() throws IOException {
    final OAbstractPaginatedStorage storage =
        (OAbstractPaginatedStorage) ODatabaseRecordThreadLocal.instance().get().getStorage();
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
  public void testSerializeWALChangesEmbeddedNonDurable() throws IOException {
    final OAbstractPaginatedStorage storage =
        (OAbstractPaginatedStorage) ODatabaseRecordThreadLocal.instance().get().getStorage();
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
          ByteBuffer.allocateDirect(len + serializationOffset).order(ByteOrder.nativeOrder());
      final byte[] data = new byte[len];
      streamSerializerSBTreeIndexRIDContainer.serializeNativeObject(indexRIDContainer, data, 0);

      final OWALChanges walChanges = new OWALChangesTree();
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
    final OAbstractPaginatedStorage storage =
        (OAbstractPaginatedStorage) ODatabaseRecordThreadLocal.instance().get().getStorage();
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
          ByteBuffer.allocateDirect(len + serializationOffset).order(ByteOrder.nativeOrder());
      final byte[] data = new byte[len];
      streamSerializerSBTreeIndexRIDContainer.serializeNativeObject(indexRIDContainer, data, 0);

      final OWALChanges walChanges = new OWALChangesTree();
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
    final OAbstractPaginatedStorage storage =
        (OAbstractPaginatedStorage) ODatabaseRecordThreadLocal.instance().get().getStorage();
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
          ByteBuffer.allocateDirect(len + serializationOffset).order(ByteOrder.nativeOrder());
      final byte[] data = new byte[len];
      streamSerializerSBTreeIndexRIDContainer.serializeNativeObject(indexRIDContainer, data, 0);

      final OWALChanges walChanges = new OWALChangesTree();
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
    final OAbstractPaginatedStorage storage =
        (OAbstractPaginatedStorage) ODatabaseRecordThreadLocal.instance().get().getStorage();
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
          ByteBuffer.allocateDirect(len + serializationOffset).order(ByteOrder.nativeOrder());
      final byte[] data = new byte[len];
      streamSerializerSBTreeIndexRIDContainer.serializeNativeObject(indexRIDContainer, data, 0);

      final OWALChanges walChanges = new OWALChangesTree();
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

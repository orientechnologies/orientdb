package com.orientechnologies.orient.core.serialization.serializer.stream;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OIndexRIDContainer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class OStreamSerializerSBTreeIndexRIDContainerTest {
  private ODatabaseDocumentTx                      database;
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
  public void testSerializeInByteBufferEmbeddedNonDurable() {
    final OIndexRIDContainer indexRIDContainer = new OIndexRIDContainer("test", false, new AtomicLong(0));

    indexRIDContainer.setTopThreshold(100);
    for (int i = 0; i < 5; i++) {
      indexRIDContainer.add(new ORecordId(1, i));
    }

    Assert.assertTrue(indexRIDContainer.isEmbedded());

    final int len = streamSerializerSBTreeIndexRIDContainer.getObjectSize(indexRIDContainer);
    final int serializationOffset = 5;

    final ByteBuffer buffer = ByteBuffer.allocate(len + serializationOffset);
    buffer.position(serializationOffset);

    streamSerializerSBTreeIndexRIDContainer.serializeInByteBufferObject(indexRIDContainer, buffer);

    final int binarySize = buffer.position() - serializationOffset;
    Assert.assertEquals(binarySize, len);

    buffer.position(serializationOffset);
    Assert.assertEquals(streamSerializerSBTreeIndexRIDContainer.getObjectSizeInByteBuffer(buffer), len);

    buffer.position(serializationOffset);
    OIndexRIDContainer newRidContainer = streamSerializerSBTreeIndexRIDContainer.deserializeFromByteBufferObject(buffer);

    Assert.assertEquals(buffer.position() - serializationOffset, len);
    Assert.assertNotSame(newRidContainer, indexRIDContainer);

    Assert.assertTrue(newRidContainer.isEmbedded());
    Assert.assertFalse(newRidContainer.isDurableNonTxMode());

    final Set<OIdentifiable> storedRids = new HashSet<OIdentifiable>(newRidContainer);
    final Set<OIdentifiable> newRids = new HashSet<OIdentifiable>(indexRIDContainer);

    Assert.assertEquals(newRids, storedRids);
  }

  @Test
  public void testSerializeInByteBufferEmbeddedDurable() {
    final OIndexRIDContainer indexRIDContainer = new OIndexRIDContainer("test", true, new AtomicLong(0));

    indexRIDContainer.setTopThreshold(100);
    for (int i = 0; i < 5; i++) {
      indexRIDContainer.add(new ORecordId(1, i * 2));
    }

    Assert.assertTrue(indexRIDContainer.isEmbedded());

    final int len = streamSerializerSBTreeIndexRIDContainer.getObjectSize(indexRIDContainer);
    final int serializationOffset = 5;

    final ByteBuffer buffer = ByteBuffer.allocate(len + serializationOffset);
    buffer.position(serializationOffset);

    streamSerializerSBTreeIndexRIDContainer.serializeInByteBufferObject(indexRIDContainer, buffer);

    final int binarySize = buffer.position() - serializationOffset;
    Assert.assertEquals(binarySize, len);

    buffer.position(serializationOffset);
    Assert.assertEquals(streamSerializerSBTreeIndexRIDContainer.getObjectSizeInByteBuffer(buffer), len);

    buffer.position(serializationOffset);
    OIndexRIDContainer newRidContainer = streamSerializerSBTreeIndexRIDContainer.deserializeFromByteBufferObject(buffer);

    Assert.assertEquals(buffer.position() - serializationOffset, len);
    Assert.assertNotSame(newRidContainer, indexRIDContainer);

    Assert.assertTrue(newRidContainer.isEmbedded());
    Assert.assertTrue(newRidContainer.isDurableNonTxMode());

    final Set<OIdentifiable> storedRids = new HashSet<OIdentifiable>(newRidContainer);
    final Set<OIdentifiable> newRids = new HashSet<OIdentifiable>(indexRIDContainer);

    Assert.assertEquals(newRids, storedRids);
  }

  @Test
  public void testSerializeInByteBufferNonEmbeddedNonDurable() {
    final OIndexRIDContainer indexRIDContainer = new OIndexRIDContainer("test", false, new AtomicLong(0));

    indexRIDContainer.setTopThreshold(1);
    for (int i = 0; i < 5; i++) {
      indexRIDContainer.add(new ORecordId(1, i * 3));
    }

    Assert.assertFalse(indexRIDContainer.isEmbedded());

    final int len = streamSerializerSBTreeIndexRIDContainer.getObjectSize(indexRIDContainer);
    final int serializationOffset = 5;

    final ByteBuffer buffer = ByteBuffer.allocate(len + serializationOffset);
    buffer.position(serializationOffset);

    streamSerializerSBTreeIndexRIDContainer.serializeInByteBufferObject(indexRIDContainer, buffer);

    final int binarySize = buffer.position() - serializationOffset;
    Assert.assertEquals(binarySize, len);

    buffer.position(serializationOffset);
    Assert.assertEquals(streamSerializerSBTreeIndexRIDContainer.getObjectSizeInByteBuffer(buffer), len);

    buffer.position(serializationOffset);
    OIndexRIDContainer newRidContainer = streamSerializerSBTreeIndexRIDContainer.deserializeFromByteBufferObject(buffer);

    Assert.assertEquals(buffer.position() - serializationOffset, len);
    Assert.assertNotSame(newRidContainer, indexRIDContainer);

    Assert.assertFalse(newRidContainer.isEmbedded());
    Assert.assertFalse(newRidContainer.isDurableNonTxMode());

    final Set<OIdentifiable> storedRids = new HashSet<OIdentifiable>(newRidContainer);
    final Set<OIdentifiable> newRids = new HashSet<OIdentifiable>(indexRIDContainer);

    Assert.assertEquals(newRids, storedRids);
  }

  @Test
  public void testSerializeInByteBufferNonEmbeddedDurable() {
    final OIndexRIDContainer indexRIDContainer = new OIndexRIDContainer("test", true, new AtomicLong(0));

    indexRIDContainer.setTopThreshold(1);
    for (int i = 0; i < 5; i++) {
      indexRIDContainer.add(new ORecordId(1, i * 4));
    }

    Assert.assertFalse(indexRIDContainer.isEmbedded());

    final int len = streamSerializerSBTreeIndexRIDContainer.getObjectSize(indexRIDContainer);
    final int serializationOffset = 5;

    final ByteBuffer buffer = ByteBuffer.allocate(len + serializationOffset);
    buffer.position(serializationOffset);

    streamSerializerSBTreeIndexRIDContainer.serializeInByteBufferObject(indexRIDContainer, buffer);

    final int binarySize = buffer.position() - serializationOffset;
    Assert.assertEquals(binarySize, len);

    buffer.position(serializationOffset);
    Assert.assertEquals(streamSerializerSBTreeIndexRIDContainer.getObjectSizeInByteBuffer(buffer), len);

    buffer.position(serializationOffset);
    OIndexRIDContainer newRidContainer = streamSerializerSBTreeIndexRIDContainer.deserializeFromByteBufferObject(buffer);

    Assert.assertEquals(buffer.position() - serializationOffset, len);
    Assert.assertNotSame(newRidContainer, indexRIDContainer);

    Assert.assertFalse(newRidContainer.isEmbedded());
    Assert.assertTrue(newRidContainer.isDurableNonTxMode());

    final Set<OIdentifiable> storedRids = new HashSet<OIdentifiable>(newRidContainer);
    final Set<OIdentifiable> newRids = new HashSet<OIdentifiable>(indexRIDContainer);

    Assert.assertEquals(newRids, storedRids);
  }
}

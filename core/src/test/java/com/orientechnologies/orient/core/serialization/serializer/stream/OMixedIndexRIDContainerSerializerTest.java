package com.orientechnologies.orient.core.serialization.serializer.stream;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OMixedIndexRIDContainer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class OMixedIndexRIDContainerSerializerTest {
  private ODatabaseDocumentTx               database;
  private OMixedIndexRIDContainerSerializer mixedIndexRIDContainerSerializer;

  @Before
  public void beforeClass() {
    database = new ODatabaseDocumentTx("memory:" + this.getClass().getSimpleName());
    database.create();
    mixedIndexRIDContainerSerializer = new OMixedIndexRIDContainerSerializer();
  }

  @After
  public void afterClass() {
    database.drop();
  }

  @Test
  public void testSerializeInByteBufferEmbeddedNonDurable() {
    final OMixedIndexRIDContainer indexRIDContainer = new OMixedIndexRIDContainer("test", new AtomicLong(0));

    indexRIDContainer.setTopThreshold(100);
    for (int i = 0; i < 5; i++) {
      indexRIDContainer.add(new ORecordId(1, i));
    }

    final int len = mixedIndexRIDContainerSerializer.getObjectSize(indexRIDContainer);
    final int serializationOffset = 5;

    final ByteBuffer buffer = ByteBuffer.allocate(len + serializationOffset);
    buffer.position(serializationOffset);

    mixedIndexRIDContainerSerializer.serializeInByteBufferObject(indexRIDContainer, buffer);

    final int binarySize = buffer.position() - serializationOffset;
    Assert.assertEquals(binarySize, len);

    buffer.position(serializationOffset);
    Assert.assertEquals(mixedIndexRIDContainerSerializer.getObjectSizeInByteBuffer(buffer), len);

    buffer.position(serializationOffset);
    OMixedIndexRIDContainer newRidContainer = mixedIndexRIDContainerSerializer.deserializeFromByteBufferObject(buffer);

    Assert.assertEquals(buffer.position() - serializationOffset, len);
    Assert.assertNotSame(newRidContainer, indexRIDContainer);

    final Set<OIdentifiable> storedRids = new HashSet<OIdentifiable>(newRidContainer);
    final Set<OIdentifiable> newRids = new HashSet<OIdentifiable>(indexRIDContainer);

    Assert.assertEquals(newRids, storedRids);
  }

  @Test
  public void testSerializeInByteBufferEmbeddedDurable() {
    final OMixedIndexRIDContainer indexRIDContainer = new OMixedIndexRIDContainer("test", new AtomicLong(0));

    indexRIDContainer.setTopThreshold(100);
    for (int i = 0; i < 5; i++) {
      indexRIDContainer.add(new ORecordId(1, i * 2));
    }

    final int len = mixedIndexRIDContainerSerializer.getObjectSize(indexRIDContainer);
    final int serializationOffset = 5;

    final ByteBuffer buffer = ByteBuffer.allocate(len + serializationOffset);
    buffer.position(serializationOffset);

    mixedIndexRIDContainerSerializer.serializeInByteBufferObject(indexRIDContainer, buffer);

    final int binarySize = buffer.position() - serializationOffset;
    Assert.assertEquals(binarySize, len);

    buffer.position(serializationOffset);
    Assert.assertEquals(mixedIndexRIDContainerSerializer.getObjectSizeInByteBuffer(buffer), len);

    buffer.position(serializationOffset);
    OMixedIndexRIDContainer newRidContainer = mixedIndexRIDContainerSerializer.deserializeFromByteBufferObject(buffer);

    Assert.assertEquals(buffer.position() - serializationOffset, len);
    Assert.assertNotSame(newRidContainer, indexRIDContainer);

    final Set<OIdentifiable> storedRids = new HashSet<OIdentifiable>(newRidContainer);
    final Set<OIdentifiable> newRids = new HashSet<OIdentifiable>(indexRIDContainer);

    Assert.assertEquals(newRids, storedRids);
  }

  @Test
  public void testSerializeInByteBufferNonEmbeddedNonDurable() {
    final OMixedIndexRIDContainer indexRIDContainer = new OMixedIndexRIDContainer("test", new AtomicLong(0));

    indexRIDContainer.setTopThreshold(1);
    for (int i = 0; i < 5; i++) {
      indexRIDContainer.add(new ORecordId(1, i * 3));
    }

    final int len = mixedIndexRIDContainerSerializer.getObjectSize(indexRIDContainer);
    final int serializationOffset = 5;

    final ByteBuffer buffer = ByteBuffer.allocate(len + serializationOffset);
    buffer.position(serializationOffset);

    mixedIndexRIDContainerSerializer.serializeInByteBufferObject(indexRIDContainer, buffer);

    final int binarySize = buffer.position() - serializationOffset;
    Assert.assertEquals(binarySize, len);

    buffer.position(serializationOffset);
    Assert.assertEquals(mixedIndexRIDContainerSerializer.getObjectSizeInByteBuffer(buffer), len);

    buffer.position(serializationOffset);
    OMixedIndexRIDContainer newRidContainer = mixedIndexRIDContainerSerializer.deserializeFromByteBufferObject(buffer);

    Assert.assertEquals(buffer.position() - serializationOffset, len);
    Assert.assertNotSame(newRidContainer, indexRIDContainer);

    final Set<OIdentifiable> storedRids = new HashSet<OIdentifiable>(newRidContainer);
    final Set<OIdentifiable> newRids = new HashSet<OIdentifiable>(indexRIDContainer);

    Assert.assertEquals(newRids, storedRids);
  }

  @Test
  public void testSerializeInByteBufferNonEmbeddedDurable() {
    final OMixedIndexRIDContainer indexRIDContainer = new OMixedIndexRIDContainer("test", new AtomicLong(0));

    indexRIDContainer.setTopThreshold(1);
    for (int i = 0; i < 5; i++) {
      indexRIDContainer.add(new ORecordId(1, i * 4));
    }

    final int len = mixedIndexRIDContainerSerializer.getObjectSize(indexRIDContainer);
    final int serializationOffset = 5;

    final ByteBuffer buffer = ByteBuffer.allocate(len + serializationOffset);
    buffer.position(serializationOffset);

    mixedIndexRIDContainerSerializer.serializeInByteBufferObject(indexRIDContainer, buffer);

    final int binarySize = buffer.position() - serializationOffset;
    Assert.assertEquals(binarySize, len);

    buffer.position(serializationOffset);
    Assert.assertEquals(mixedIndexRIDContainerSerializer.getObjectSizeInByteBuffer(buffer), len);

    buffer.position(serializationOffset);
    OMixedIndexRIDContainer newRidContainer = mixedIndexRIDContainerSerializer.deserializeFromByteBufferObject(buffer);

    Assert.assertEquals(buffer.position() - serializationOffset, len);
    Assert.assertNotSame(newRidContainer, indexRIDContainer);

    final Set<OIdentifiable> storedRids = new HashSet<OIdentifiable>(newRidContainer);
    final Set<OIdentifiable> newRids = new HashSet<OIdentifiable>(indexRIDContainer);

    Assert.assertEquals(newRids, storedRids);
  }

}

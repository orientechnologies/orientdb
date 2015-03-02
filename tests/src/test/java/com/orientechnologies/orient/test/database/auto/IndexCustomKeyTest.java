/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OBinaryTypeSerializer;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.ORuntimeKeyIndexDefinition;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;
import com.orientechnologies.orient.core.tx.OTransaction;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.Arrays;

@Test(groups = { "index" })
public class IndexCustomKeyTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public IndexCustomKeyTest(@Optional String url) {
    super(url);
  }

  public static class ComparableBinary implements Comparable<ComparableBinary>, OSerializableStream {
    private byte[] value;

    public ComparableBinary() {
    }

    public ComparableBinary(byte[] buffer) {
      value = buffer;
    }

    @Override
    public int compareTo(ComparableBinary o) {
      final int size = value.length;

      for (int i = 0; i < size; ++i) {
        if (value[i] > o.value[i])
          return 1;
        else if (value[i] < o.value[i])
          return -1;
      }
      return 0;
    }

    public byte[] toByteArray() {
      return value;
    }

    @Override
    public byte[] toStream() throws OSerializationException {
      return value;
    }

    @Override
    public OSerializableStream fromStream(byte[] iStream) throws OSerializationException {
      this.value = iStream;
      return this;
    }
  }

  public static class OHash256Serializer implements OBinarySerializer<ComparableBinary> {

    public static final OBinaryTypeSerializer INSTANCE = new OBinaryTypeSerializer();
    public static final byte                  ID       = 100;
    public static final int                   LENGTH   = 32;

    public int getObjectSize(final int length) {
      return length;
    }

    @Override
    public int getObjectSize(final ComparableBinary object, Object... hints) {
      return object.toByteArray().length;
    }

    @Override
    public void serialize(final ComparableBinary object, final byte[] stream, final int startPosition, Object... hints) {
      final byte[] buffer = object.toByteArray();
      System.arraycopy(buffer, 0, stream, startPosition, buffer.length);
    }

    @Override
    public ComparableBinary deserialize(final byte[] stream, final int startPosition) {
      final byte[] buffer = Arrays.copyOfRange(stream, startPosition, startPosition + LENGTH);
      return new ComparableBinary(buffer);
    }

    @Override
    public int getObjectSize(byte[] stream, int startPosition) {
      return LENGTH;
    }

    @Override
    public byte getId() {
      return ID;
    }

    @Override
    public int getObjectSizeNative(byte[] stream, int startPosition) {
      return LENGTH;
    }

    @Override
    public void serializeNativeObject(ComparableBinary object, byte[] stream, int startPosition, Object... hints) {
      serialize(object, stream, startPosition);
    }

    @Override
    public ComparableBinary deserializeNativeObject(byte[] stream, int startPosition) {
      return deserialize(stream, startPosition);
    }

    @Override
    public void serializeInDirectMemoryObject(ComparableBinary object, ODirectMemoryPointer pointer, long offset, Object... hints) {
      final byte[] buffer = object.toByteArray();
      pointer.set(offset, buffer, 0, buffer.length);
    }

    @Override
    public ComparableBinary deserializeFromDirectMemoryObject(ODirectMemoryPointer pointer, long offset) {
      return new ComparableBinary(pointer.get(offset, LENGTH));
    }

    @Override
    public ComparableBinary deserializeFromDirectMemoryObject(OWALChangesTree.PointerWrapper wrapper, long offset) {
      return new ComparableBinary(wrapper.get(offset, LENGTH));
    }

    @Override
    public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
      return LENGTH;
    }

    @Override
    public int getObjectSizeInDirectMemory(OWALChangesTree.PointerWrapper wrapper, long offset) {
      return LENGTH;
    }

    @Override
    public boolean isFixedLength() {
      return true;
    }

    @Override
    public int getFixedLength() {
      return LENGTH;
    }

    @Override
    public ComparableBinary preprocess(ComparableBinary value, Object... hints) {
      return value;
    }
  }

  protected OIndex<?> getIndex() {
    return database.getMetadata().getIndexManager().getIndex("custom-hash");
  }

  @AfterClass
  @Override
  public void afterClass() throws Exception {
    if (database.isClosed())
      database.open("admin", "admin");

    database.getMetadata().getIndexManager().dropIndex("custom-hash");
    database.close();

    super.afterClass();
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    super.beforeMethod();

    OIndex<?> index = getIndex();

    if (index == null) {
      OBinarySerializerFactory.getInstance().registerSerializer(new OHash256Serializer(), null);

      database.getMetadata().getIndexManager()
          .createIndex("custom-hash", "UNIQUE", new ORuntimeKeyIndexDefinition(OHash256Serializer.ID), null, null, null);
    }
  }

  public void testUsage() {
    OIndex<?> index = getIndex();
    ComparableBinary key1 = new ComparableBinary(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
        3, 4, 5, 6, 7, 8, 9, 0, 1 });
    ODocument doc1 = new ODocument().field("k", "key1");
    index.put(key1, doc1);

    ComparableBinary key2 = new ComparableBinary(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
        3, 4, 5, 6, 7, 8, 9, 0, 2 });
    ODocument doc2 = new ODocument().field("k", "key1");
    index.put(key2, doc2);

    Assert.assertEquals(index.get(key1), doc1);
    Assert.assertEquals(index.get(key2), doc2);
  }

  public void testTransactionalUsageWorks() {
    database.begin(OTransaction.TXTYPE.OPTIMISTIC);
    // OIndex<?> index = getManualIndex();
    ComparableBinary key3 = new ComparableBinary(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
        3, 4, 5, 6, 7, 8, 9, 0, 3 });
    ODocument doc1 = new ODocument().field("k", "key3");

    final OIndex index = getIndex();
    index.put(key3, doc1);

    ComparableBinary key4 = new ComparableBinary(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
        3, 4, 5, 6, 7, 8, 9, 0, 4 });
    ODocument doc2 = new ODocument().field("k", "key4");
    index.put(key4, doc2);

    database.commit();

    Assert.assertEquals(index.get(key3), doc1);
    Assert.assertEquals(index.get(key4), doc2);
  }

  @Test(dependsOnMethods = { "testTransactionalUsageWorks" })
  public void testTransactionalUsageBreaks1() {
    database.begin(OTransaction.TXTYPE.OPTIMISTIC);
    OIndex<?> index = getIndex();
    ComparableBinary key5 = new ComparableBinary(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
        3, 4, 5, 6, 7, 8, 9, 0, 5 });
    ODocument doc1 = new ODocument().field("k", "key5");
    index.put(key5, doc1);

    ComparableBinary key6 = new ComparableBinary(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
        3, 4, 5, 6, 7, 8, 9, 0, 6 });
    ODocument doc2 = new ODocument().field("k", "key6");
    index.put(key6, doc2);

    database.commit();

    Assert.assertEquals(index.get(key5), doc1);
    Assert.assertEquals(index.get(key6), doc2);
  }

  @Test(dependsOnMethods = { "testTransactionalUsageWorks" })
  public void testTransactionalUsageBreaks2() {
    OIndex<?> index = getIndex();
    database.begin(OTransaction.TXTYPE.OPTIMISTIC);
    ComparableBinary key7 = new ComparableBinary(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
        3, 4, 5, 6, 7, 8, 9, 0, 7 });
    ODocument doc1 = new ODocument().field("k", "key7");
    index.put(key7, doc1);

    ComparableBinary key8 = new ComparableBinary(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2,
        3, 4, 5, 6, 7, 8, 9, 0, 8 });
    ODocument doc2 = new ODocument().field("k", "key8");
    index.put(key8, doc2);

    database.commit();

    Assert.assertEquals(index.get(key7), doc1);
    Assert.assertEquals(index.get(key8), doc2);
  }

  public void testUsage2() {
    // EMPTY BUT IT'S ENOUGH TO CALL THE BEFORE_METHOD AND TRY LOADING IT
  }
}

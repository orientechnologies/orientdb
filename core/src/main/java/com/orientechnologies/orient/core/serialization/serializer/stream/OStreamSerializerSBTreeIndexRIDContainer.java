/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.serialization.serializer.stream;

import static com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer.RID_SIZE;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OBooleanSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OIndexRIDContainer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OIndexRIDContainerSBTree;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

public class OStreamSerializerSBTreeIndexRIDContainer
    implements OBinarySerializer<OIndexRIDContainer> {
  public static final OStreamSerializerSBTreeIndexRIDContainer INSTANCE =
      new OStreamSerializerSBTreeIndexRIDContainer();

  public static final byte ID = 21;
  public static final int FILE_ID_OFFSET = 0;
  public static final int EMBEDDED_OFFSET = FILE_ID_OFFSET + OLongSerializer.LONG_SIZE;
  public static final int DURABLE_OFFSET = EMBEDDED_OFFSET + OBooleanSerializer.BOOLEAN_SIZE;
  public static final int SBTREE_ROOTINDEX_OFFSET =
      DURABLE_OFFSET + OBooleanSerializer.BOOLEAN_SIZE;
  public static final int SBTREE_ROOTOFFSET_OFFSET =
      SBTREE_ROOTINDEX_OFFSET + OLongSerializer.LONG_SIZE;

  public static final int EMBEDDED_SIZE_OFFSET = DURABLE_OFFSET + OBooleanSerializer.BOOLEAN_SIZE;
  public static final int EMBEDDED_VALUES_OFFSET =
      EMBEDDED_SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  public static final OLongSerializer LONG_SERIALIZER = OLongSerializer.INSTANCE;
  public static final OBooleanSerializer BOOLEAN_SERIALIZER = OBooleanSerializer.INSTANCE;
  public static final OIntegerSerializer INT_SERIALIZER = OIntegerSerializer.INSTANCE;
  public static final int SBTREE_CONTAINER_SIZE =
      2 * OBooleanSerializer.BOOLEAN_SIZE
          + 2 * OLongSerializer.LONG_SIZE
          + OIntegerSerializer.INT_SIZE;
  public static final OLinkSerializer LINK_SERIALIZER = OLinkSerializer.INSTANCE;

  @Override
  public int getObjectSize(OIndexRIDContainer object, Object... hints) {
    if (object.isEmbedded()) {
      return embeddedObjectSerializedSize(object.size());
    } else {
      return SBTREE_CONTAINER_SIZE;
    }
  }

  @Override
  public int getObjectSize(byte[] stream, int startPosition) {
    throw new UnsupportedOperationException("not implemented yet");
  }

  @Override
  public void serialize(
      OIndexRIDContainer object, byte[] stream, int startPosition, Object... hints) {
    throw new UnsupportedOperationException("not implemented yet");
  }

  @Override
  public OIndexRIDContainer deserialize(byte[] stream, int startPosition) {
    throw new UnsupportedOperationException("not implemented yet");
  }

  @Override
  public byte getId() {
    return ID;
  }

  @Override
  public boolean isFixedLength() {
    return false;
  }

  @Override
  public int getFixedLength() {
    throw new UnsupportedOperationException("Length is not fixed");
  }

  @Override
  public void serializeNativeObject(
      OIndexRIDContainer object, byte[] stream, int offset, Object... hints) {
    LONG_SERIALIZER.serializeNative(object.getFileId(), stream, offset + FILE_ID_OFFSET);

    final boolean embedded = object.isEmbedded();
    final boolean durable = object.isDurableNonTxMode();

    BOOLEAN_SERIALIZER.serializeNative(embedded, stream, offset + EMBEDDED_OFFSET);
    BOOLEAN_SERIALIZER.serializeNative(durable, stream, offset + DURABLE_OFFSET);

    if (embedded) {
      INT_SERIALIZER.serializeNative(object.size(), stream, offset + EMBEDDED_SIZE_OFFSET);

      int p = offset + EMBEDDED_VALUES_OFFSET;
      for (OIdentifiable ids : object) {
        LINK_SERIALIZER.serializeNativeObject(ids, stream, p);
        p += RID_SIZE;
      }
    } else {
      final OIndexRIDContainerSBTree underlying = (OIndexRIDContainerSBTree) object.getUnderlying();
      final OBonsaiBucketPointer rootPointer = underlying.getRootPointer();
      LONG_SERIALIZER.serializeNative(
          rootPointer.getPageIndex(), stream, offset + SBTREE_ROOTINDEX_OFFSET);
      INT_SERIALIZER.serializeNative(
          rootPointer.getPageOffset(), stream, offset + SBTREE_ROOTOFFSET_OFFSET);
    }
  }

  @Override
  public OIndexRIDContainer deserializeNativeObject(byte[] stream, int offset) {
    final long fileId = LONG_SERIALIZER.deserializeNative(stream, offset + FILE_ID_OFFSET);
    final boolean durable = BOOLEAN_SERIALIZER.deserializeNative(stream, offset + DURABLE_OFFSET);

    if (BOOLEAN_SERIALIZER.deserializeNative(stream, offset + EMBEDDED_OFFSET)) {
      final int size = INT_SERIALIZER.deserializeNative(stream, offset + EMBEDDED_SIZE_OFFSET);
      final Set<OIdentifiable> underlying =
          new HashSet<OIdentifiable>(Math.max((int) (size / .75f) + 1, 16));

      int p = offset + EMBEDDED_VALUES_OFFSET;
      for (int i = 0; i < size; i++) {
        underlying.add(LINK_SERIALIZER.deserializeNativeObject(stream, p));
        p += RID_SIZE;
      }

      return new OIndexRIDContainer(fileId, underlying, durable);
    } else {
      final long pageIndex =
          LONG_SERIALIZER.deserializeNative(stream, offset + SBTREE_ROOTINDEX_OFFSET);
      final int pageOffset =
          INT_SERIALIZER.deserializeNative(stream, offset + SBTREE_ROOTOFFSET_OFFSET);
      final OBonsaiBucketPointer rootPointer = new OBonsaiBucketPointer(pageIndex, pageOffset);
      final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();
      final OIndexRIDContainerSBTree underlying =
          new OIndexRIDContainerSBTree(
              fileId, rootPointer, (OAbstractPaginatedStorage) db.getStorage());
      return new OIndexRIDContainer(fileId, underlying, durable);
    }
  }

  @Override
  public int getObjectSizeNative(byte[] stream, int startPosition) {
    throw new UnsupportedOperationException("not implemented yet");
  }

  @Override
  public OIndexRIDContainer preprocess(OIndexRIDContainer value, Object... hints) {
    return value;
  }

  private int embeddedObjectSerializedSize(int size) {
    return OLongSerializer.LONG_SIZE
        + 2 * OBooleanSerializer.BOOLEAN_SIZE
        + OIntegerSerializer.INT_SIZE
        + size * RID_SIZE;
  }

  /** {@inheritDoc} */
  @Override
  public void serializeInByteBufferObject(
      OIndexRIDContainer object, ByteBuffer buffer, Object... hints) {
    buffer.putLong(object.getFileId());

    final boolean embedded = object.isEmbedded();
    final boolean durable = object.isDurableNonTxMode();

    buffer.put((byte) (embedded ? 1 : 0));
    buffer.put((byte) (durable ? 1 : 0));

    if (embedded) {
      buffer.putInt(object.size());

      for (OIdentifiable ids : object) {
        LINK_SERIALIZER.serializeInByteBufferObject(ids, buffer);
      }
    } else {
      final OIndexRIDContainerSBTree underlying = (OIndexRIDContainerSBTree) object.getUnderlying();
      final OBonsaiBucketPointer rootPointer = underlying.getRootPointer();

      buffer.putLong(rootPointer.getPageIndex());
      buffer.putInt(rootPointer.getPageOffset());
    }
  }

  /** {@inheritDoc} */
  @Override
  public OIndexRIDContainer deserializeFromByteBufferObject(ByteBuffer buffer) {
    final long fileId = buffer.getLong();
    final boolean embedded = buffer.get() > 0;
    final boolean durable = buffer.get() > 0;

    if (embedded) {
      final int size = buffer.getInt();
      final Set<OIdentifiable> underlying =
          new HashSet<OIdentifiable>(Math.max((int) (size / .75f) + 1, 16));

      for (int i = 0; i < size; i++) {
        underlying.add(LINK_SERIALIZER.deserializeFromByteBufferObject(buffer));
      }

      return new OIndexRIDContainer(fileId, underlying, durable);
    } else {
      final long pageIndex = buffer.getLong();
      final int pageOffset = buffer.getInt();

      final OBonsaiBucketPointer rootPointer = new OBonsaiBucketPointer(pageIndex, pageOffset);
      final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();
      final OIndexRIDContainerSBTree underlying =
          new OIndexRIDContainerSBTree(
              fileId, rootPointer, (OAbstractPaginatedStorage) db.getStorage());
      return new OIndexRIDContainer(fileId, underlying, durable);
    }
  }

  /** {@inheritDoc} */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    final int offset = buffer.position();
    buffer.position();
    if (buffer.get(offset + EMBEDDED_OFFSET) > 0) {
      return embeddedObjectSerializedSize(buffer.getInt(offset + EMBEDDED_SIZE_OFFSET));
    } else {
      return SBTREE_CONTAINER_SIZE;
    }
  }

  /** {@inheritDoc} */
  @Override
  public OIndexRIDContainer deserializeFromByteBufferObject(
      ByteBuffer buffer, OWALChanges walChanges, int offset) {
    final long fileId = walChanges.getLongValue(buffer, offset + FILE_ID_OFFSET);
    final boolean durable = walChanges.getByteValue(buffer, offset + DURABLE_OFFSET) > 0;

    if (walChanges.getByteValue(buffer, offset + EMBEDDED_OFFSET) > 0) {
      final int size = walChanges.getIntValue(buffer, offset + EMBEDDED_SIZE_OFFSET);
      final Set<OIdentifiable> underlying =
          new HashSet<OIdentifiable>(Math.max((int) (size / .75f) + 1, 16));

      int p = offset + EMBEDDED_VALUES_OFFSET;
      for (int i = 0; i < size; i++) {
        underlying.add(LINK_SERIALIZER.deserializeFromByteBufferObject(buffer, walChanges, p));
        p += RID_SIZE;
      }

      return new OIndexRIDContainer(fileId, underlying, durable);
    } else {
      final long pageIndex = walChanges.getLongValue(buffer, offset + SBTREE_ROOTINDEX_OFFSET);
      final int pageOffset = walChanges.getIntValue(buffer, offset + SBTREE_ROOTOFFSET_OFFSET);
      final OBonsaiBucketPointer rootPointer = new OBonsaiBucketPointer(pageIndex, pageOffset);
      final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();
      final OIndexRIDContainerSBTree underlying =
          new OIndexRIDContainerSBTree(
              fileId, rootPointer, (OAbstractPaginatedStorage) db.getStorage());
      return new OIndexRIDContainer(fileId, underlying, durable);
    }
  }

  /** {@inheritDoc} */
  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    if (walChanges.getByteValue(buffer, offset + EMBEDDED_OFFSET) > 0) {
      return embeddedObjectSerializedSize(
          walChanges.getIntValue(buffer, offset + EMBEDDED_SIZE_OFFSET));
    } else {
      return SBTREE_CONTAINER_SIZE;
    }
  }
}

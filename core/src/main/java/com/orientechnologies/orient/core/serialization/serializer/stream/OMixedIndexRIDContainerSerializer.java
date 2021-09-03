package com.orientechnologies.orient.core.serialization.serializer.stream;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OCompactedLinkSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OIndexRIDContainerSBTree;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OMixedIndexRIDContainer;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

public class OMixedIndexRIDContainerSerializer
    implements OBinarySerializer<OMixedIndexRIDContainer> {
  public static final OMixedIndexRIDContainerSerializer INSTANCE =
      new OMixedIndexRIDContainerSerializer();

  public static final byte ID = 23;

  @Override
  public int getObjectSize(OMixedIndexRIDContainer object, Object... hints) {
    int size =
        OLongSerializer.LONG_SIZE
            + OIntegerSerializer.INT_SIZE
            + OIntegerSerializer.INT_SIZE; // total size + fileId + embedded Size
    size += OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE; // root offset and page index

    final Set<ORID> embedded = object.getEmbeddedSet();
    for (ORID orid : embedded) {
      size += OCompactedLinkSerializer.INSTANCE.getObjectSize(orid);
    }

    return size;
  }

  @Override
  public int getObjectSize(byte[] stream, int startPosition) {
    return OIntegerSerializer.INSTANCE.deserialize(stream, startPosition);
  }

  @Override
  public void serialize(
      OMixedIndexRIDContainer object, byte[] stream, int startPosition, Object... hints) {
    int size =
        OLongSerializer.LONG_SIZE
            + OIntegerSerializer.INT_SIZE
            + OIntegerSerializer.INT_SIZE; // total size + fileId + embedded Size
    size += OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE; // root offset and page index

    final Set<ORID> embedded = object.getEmbeddedSet();
    for (ORID orid : embedded) {
      size += OCompactedLinkSerializer.INSTANCE.getObjectSize(orid);
    }

    OIntegerSerializer.INSTANCE.serialize(size, stream, startPosition);
    startPosition += OIntegerSerializer.INT_SIZE;

    OLongSerializer.INSTANCE.serialize(object.getFileId(), stream, startPosition);
    startPosition += OLongSerializer.LONG_SIZE;

    OIntegerSerializer.INSTANCE.serialize(embedded.size(), stream, startPosition);
    startPosition += OIntegerSerializer.INT_SIZE;

    for (ORID orid : embedded) {
      OCompactedLinkSerializer.INSTANCE.serialize(orid, stream, startPosition);
      startPosition += OCompactedLinkSerializer.INSTANCE.getObjectSize(stream, startPosition);
    }

    final OIndexRIDContainerSBTree tree = object.getTree();
    if (tree == null) {
      OLongSerializer.INSTANCE.serialize(-1L, stream, startPosition);
      startPosition += OLongSerializer.LONG_SIZE;

      OIntegerSerializer.INSTANCE.serialize(-1, stream, startPosition);
    } else {
      final OBonsaiBucketPointer rootPointer = tree.getRootPointer();
      OLongSerializer.INSTANCE.serialize(rootPointer.getPageIndex(), stream, startPosition);
      startPosition += OLongSerializer.LONG_SIZE;

      OIntegerSerializer.INSTANCE.serialize(rootPointer.getPageOffset(), stream, startPosition);
    }
  }

  @Override
  public OMixedIndexRIDContainer deserialize(byte[] stream, int startPosition) {
    startPosition += OIntegerSerializer.INT_SIZE;

    final long fileId = OLongSerializer.INSTANCE.deserialize(stream, startPosition);
    startPosition += OLongSerializer.LONG_SIZE;

    final int embeddedSize = OIntegerSerializer.INSTANCE.deserialize(stream, startPosition);
    startPosition += OIntegerSerializer.INT_SIZE;

    final Set<ORID> hashSet = new HashSet<>();
    for (int i = 0; i < embeddedSize; i++) {
      final ORID orid =
          OCompactedLinkSerializer.INSTANCE.deserialize(stream, startPosition).getIdentity();
      startPosition += OCompactedLinkSerializer.INSTANCE.getObjectSize(stream, startPosition);
      hashSet.add(orid);
    }

    final long pageIndex = OLongSerializer.INSTANCE.deserialize(stream, startPosition);
    startPosition += OLongSerializer.LONG_SIZE;

    final int offset = OIntegerSerializer.INSTANCE.deserialize(stream, startPosition);

    final OIndexRIDContainerSBTree tree;
    if (pageIndex == -1) {
      tree = null;
    } else {
      final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();
      tree =
          new OIndexRIDContainerSBTree(
              fileId,
              new OBonsaiBucketPointer(pageIndex, offset),
              (OAbstractPaginatedStorage) db.getStorage());
    }

    return new OMixedIndexRIDContainer(fileId, hashSet, tree);
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
    return 0;
  }

  @Override
  public void serializeNativeObject(
      OMixedIndexRIDContainer object, byte[] stream, int startPosition, Object... hints) {
    int size =
        OLongSerializer.LONG_SIZE
            + OIntegerSerializer.INT_SIZE
            + OIntegerSerializer.INT_SIZE; // total size + fileId + embedded Size
    size += OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE; // root offset and page index

    final Set<ORID> embedded = object.getEmbeddedSet();
    for (ORID orid : embedded) {
      size += OCompactedLinkSerializer.INSTANCE.getObjectSize(orid);
    }

    OIntegerSerializer.INSTANCE.serializeNative(size, stream, startPosition);
    startPosition += OIntegerSerializer.INT_SIZE;

    OLongSerializer.INSTANCE.serializeNative(object.getFileId(), stream, startPosition);
    startPosition += OLongSerializer.LONG_SIZE;

    OIntegerSerializer.INSTANCE.serializeNative(embedded.size(), stream, startPosition);
    startPosition += OIntegerSerializer.INT_SIZE;

    for (ORID orid : embedded) {
      OCompactedLinkSerializer.INSTANCE.serializeNativeObject(orid, stream, startPosition);
      startPosition += OCompactedLinkSerializer.INSTANCE.getObjectSizeNative(stream, startPosition);
    }

    final OIndexRIDContainerSBTree tree = object.getTree();
    if (tree == null) {
      OLongSerializer.INSTANCE.serializeNative(-1L, stream, startPosition);
      startPosition += OLongSerializer.LONG_SIZE;

      OIntegerSerializer.INSTANCE.serializeNative(-1, stream, startPosition);
    } else {
      final OBonsaiBucketPointer rootPointer = tree.getRootPointer();
      OLongSerializer.INSTANCE.serializeNative(rootPointer.getPageIndex(), stream, startPosition);
      startPosition += OLongSerializer.LONG_SIZE;

      OIntegerSerializer.INSTANCE.serializeNative(
          rootPointer.getPageOffset(), stream, startPosition);
    }
  }

  @Override
  public OMixedIndexRIDContainer deserializeNativeObject(byte[] stream, int startPosition) {
    startPosition += OIntegerSerializer.INT_SIZE;

    final long fileId = OLongSerializer.INSTANCE.deserializeNative(stream, startPosition);
    startPosition += OLongSerializer.LONG_SIZE;

    final int embeddedSize = OIntegerSerializer.INSTANCE.deserializeNative(stream, startPosition);
    startPosition += OIntegerSerializer.INT_SIZE;

    final Set<ORID> hashSet = new HashSet<>();
    for (int i = 0; i < embeddedSize; i++) {
      final ORID orid =
          OCompactedLinkSerializer.INSTANCE
              .deserializeNativeObject(stream, startPosition)
              .getIdentity();
      startPosition += OCompactedLinkSerializer.INSTANCE.getObjectSizeNative(stream, startPosition);
      hashSet.add(orid);
    }

    final long pageIndex = OLongSerializer.INSTANCE.deserializeNative(stream, startPosition);
    startPosition += OLongSerializer.LONG_SIZE;

    final int offset = OIntegerSerializer.INSTANCE.deserializeNative(stream, startPosition);

    final OIndexRIDContainerSBTree tree;
    if (pageIndex == -1) {
      tree = null;
    } else {
      final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();
      tree =
          new OIndexRIDContainerSBTree(
              fileId,
              new OBonsaiBucketPointer(pageIndex, offset),
              (OAbstractPaginatedStorage) db.getStorage());
    }

    return new OMixedIndexRIDContainer(fileId, hashSet, tree);
  }

  @Override
  public int getObjectSizeNative(byte[] stream, int startPosition) {
    return OIntegerSerializer.INSTANCE.deserializeNative(stream, startPosition);
  }

  @Override
  public OMixedIndexRIDContainer preprocess(OMixedIndexRIDContainer value, Object... hints) {
    return value;
  }

  @Override
  public void serializeInByteBufferObject(
      OMixedIndexRIDContainer object, ByteBuffer buffer, Object... hints) {
    int size =
        OLongSerializer.LONG_SIZE
            + OIntegerSerializer.INT_SIZE
            + OIntegerSerializer.INT_SIZE; // total size + fileId + embedded Size
    size += OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE; // root offset and page index

    final Set<ORID> embedded = object.getEmbeddedSet();
    for (ORID orid : embedded) {
      size += OCompactedLinkSerializer.INSTANCE.getObjectSize(orid);
    }

    buffer.putInt(size);
    buffer.putLong(object.getFileId());
    buffer.putInt(embedded.size());

    for (ORID orid : embedded) {
      OCompactedLinkSerializer.INSTANCE.serializeInByteBufferObject(orid, buffer);
    }

    final OIndexRIDContainerSBTree tree = object.getTree();
    if (tree == null) {
      buffer.putLong(-1);
      buffer.putInt(-1);
    } else {
      final OBonsaiBucketPointer rootPointer = tree.getRootPointer();
      buffer.putLong(rootPointer.getPageIndex());
      buffer.putInt(rootPointer.getPageOffset());
    }
  }

  @Override
  public OMixedIndexRIDContainer deserializeFromByteBufferObject(ByteBuffer buffer) {
    buffer.position(buffer.position() + OIntegerSerializer.INT_SIZE);

    final long fileId = buffer.getLong();
    final int embeddedSize = buffer.getInt();

    final Set<ORID> hashSet = new HashSet<>();
    for (int i = 0; i < embeddedSize; i++) {
      final ORID orid =
          OCompactedLinkSerializer.INSTANCE.deserializeFromByteBufferObject(buffer).getIdentity();
      hashSet.add(orid);
    }

    final long pageIndex = buffer.getLong();
    final int offset = buffer.getInt();

    final OIndexRIDContainerSBTree tree;
    if (pageIndex == -1) {
      tree = null;
    } else {
      final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();
      tree =
          new OIndexRIDContainerSBTree(
              fileId,
              new OBonsaiBucketPointer(pageIndex, offset),
              (OAbstractPaginatedStorage) db.getStorage());
    }

    return new OMixedIndexRIDContainer(fileId, hashSet, tree);
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer) {
    return buffer.getInt();
  }

  @Override
  public OMixedIndexRIDContainer deserializeFromByteBufferObject(
      ByteBuffer buffer, OWALChanges walChanges, int offset) {
    offset += OIntegerSerializer.INT_SIZE;

    final long fileId = walChanges.getLongValue(buffer, offset);
    offset += OLongSerializer.LONG_SIZE;

    final int embeddedSize = walChanges.getIntValue(buffer, offset);
    offset += OIntegerSerializer.INT_SIZE;

    final Set<ORID> hashSet = new HashSet<>();
    for (int i = 0; i < embeddedSize; i++) {
      final ORID orid =
          OCompactedLinkSerializer.INSTANCE
              .deserializeFromByteBufferObject(buffer, walChanges, offset)
              .getIdentity();
      offset +=
          OCompactedLinkSerializer.INSTANCE.getObjectSizeInByteBuffer(buffer, walChanges, offset);
      hashSet.add(orid);
    }

    final long pageIndex = walChanges.getLongValue(buffer, offset);
    offset += OLongSerializer.LONG_SIZE;

    final int pageOffset = walChanges.getIntValue(buffer, offset);

    final OIndexRIDContainerSBTree tree;
    if (pageIndex == -1) {
      tree = null;
    } else {
      final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();
      tree =
          new OIndexRIDContainerSBTree(
              fileId,
              new OBonsaiBucketPointer(pageIndex, pageOffset),
              (OAbstractPaginatedStorage) db.getStorage());
    }

    return new OMixedIndexRIDContainer(fileId, hashSet, tree);
  }

  @Override
  public int getObjectSizeInByteBuffer(ByteBuffer buffer, OWALChanges walChanges, int offset) {
    return walChanges.getIntValue(buffer, offset);
  }
}

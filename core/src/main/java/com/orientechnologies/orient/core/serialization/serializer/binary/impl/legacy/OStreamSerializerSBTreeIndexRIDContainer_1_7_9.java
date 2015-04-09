/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.core.serialization.serializer.binary.impl.legacy;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OBooleanSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OIndexRIDContainer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OIndexRIDContainerSBTree;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer.RID_SIZE;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 9/24/14
 */
public class OStreamSerializerSBTreeIndexRIDContainer_1_7_9 implements OStreamSerializer, OBinarySerializer<OIndexRIDContainer> {
  public static final String                                         NAME                     = "icn";
  public static final OStreamSerializerSBTreeIndexRIDContainer_1_7_9 INSTANCE                 = new OStreamSerializerSBTreeIndexRIDContainer_1_7_9();

  public static final byte                                           ID                       = 21;
  public static final int                                            FILE_ID_OFFSET           = 0;
  public static final int                                            EMBEDDED_OFFSET          = FILE_ID_OFFSET
                                                                                                  + OLongSerializer.LONG_SIZE;
  public static final int                                            SBTREE_ROOTINDEX_OFFSET  = EMBEDDED_OFFSET
                                                                                                  + OBooleanSerializer.BOOLEAN_SIZE;
  public static final int                                            SBTREE_ROOTOFFSET_OFFSET = SBTREE_ROOTINDEX_OFFSET
                                                                                                  + OLongSerializer.LONG_SIZE;

  public static final int                                            EMBEDDED_SIZE_OFFSET     = EMBEDDED_OFFSET
                                                                                                  + OBooleanSerializer.BOOLEAN_SIZE;
  public static final int                                            EMBEDDED_VALUES_OFFSET   = EMBEDDED_SIZE_OFFSET
                                                                                                  + OIntegerSerializer.INT_SIZE;

  public static final OLongSerializer                                LONG_SERIALIZER          = OLongSerializer.INSTANCE;
  public static final OBooleanSerializer                             BOOLEAN_SERIALIZER       = OBooleanSerializer.INSTANCE;
  public static final OIntegerSerializer                             INT_SERIALIZER           = OIntegerSerializer.INSTANCE;
  public static final int                                            SBTREE_CONTAINER_SIZE    = OBooleanSerializer.BOOLEAN_SIZE + 2
                                                                                                  * OLongSerializer.LONG_SIZE
                                                                                                  + OIntegerSerializer.INT_SIZE;
  public static final OLinkSerializer                                LINK_SERIALIZER          = OLinkSerializer.INSTANCE;

  public Object fromStream(final byte[] iStream) throws IOException {
    if (iStream == null)
      return null;

    throw new UnsupportedOperationException("not implemented yet");
  }

  public byte[] toStream(final Object iObject) throws IOException {
    if (iObject == null)
      return null;

    throw new UnsupportedOperationException("not implemented yet");
  }

  public String getName() {
    return NAME;
  }

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
  public void serialize(OIndexRIDContainer object, byte[] stream, int startPosition, Object... hints) {
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
  public void serializeNativeObject(OIndexRIDContainer object, byte[] stream, int offset, Object... hints) {
    LONG_SERIALIZER.serializeNative(object.getFileId(), stream, offset + FILE_ID_OFFSET);

    final boolean embedded = object.isEmbedded();
    BOOLEAN_SERIALIZER.serializeNative(embedded, stream, offset + EMBEDDED_OFFSET);

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
      LONG_SERIALIZER.serializeNative(rootPointer.getPageIndex(), stream, offset + SBTREE_ROOTINDEX_OFFSET);
      INT_SERIALIZER.serializeNative(rootPointer.getPageOffset(), stream, offset + SBTREE_ROOTOFFSET_OFFSET);
    }
  }

  @Override
  public OIndexRIDContainer deserializeNativeObject(byte[] stream, int offset) {
    final long fileId = LONG_SERIALIZER.deserializeNative(stream, offset + FILE_ID_OFFSET);
    if (BOOLEAN_SERIALIZER.deserializeNative(stream, offset + EMBEDDED_OFFSET)) {
      final int size = INT_SERIALIZER.deserializeNative(stream, offset + EMBEDDED_SIZE_OFFSET);
      final Set<OIdentifiable> underlying = new HashSet<OIdentifiable>(Math.max((int) (size / .75f) + 1, 16));

      int p = offset + EMBEDDED_VALUES_OFFSET;
      for (int i = 0; i < size; i++) {
        underlying.add(LINK_SERIALIZER.deserializeNativeObject(stream, p));
        p += RID_SIZE;
      }

      return new OIndexRIDContainer(fileId, underlying, false);
    } else {
      final long pageIndex = LONG_SERIALIZER.deserializeNative(stream, offset + SBTREE_ROOTINDEX_OFFSET);
      final int pageOffset = INT_SERIALIZER.deserializeNative(stream, offset + SBTREE_ROOTOFFSET_OFFSET);
      final OBonsaiBucketPointer rootPointer = new OBonsaiBucketPointer(pageIndex, pageOffset);

      final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.get();
      final OIndexRIDContainerSBTree underlying = new OIndexRIDContainerSBTree(fileId, rootPointer, false,
          (OAbstractPaginatedStorage) db.getStorage());
      return new OIndexRIDContainer(fileId, underlying, false);
    }
  }

  @Override
  public int getObjectSizeNative(byte[] stream, int startPosition) {
    throw new UnsupportedOperationException("not implemented yet");
  }

  @Override
  public void serializeInDirectMemoryObject(OIndexRIDContainer object, ODirectMemoryPointer pointer, long offset, Object... hints) {
    LONG_SERIALIZER.serializeInDirectMemory(object.getFileId(), pointer, offset + FILE_ID_OFFSET);

    final boolean embedded = object.isEmbedded();
    BOOLEAN_SERIALIZER.serializeInDirectMemoryObject(embedded, pointer, offset + EMBEDDED_OFFSET);

    if (embedded) {
      INT_SERIALIZER.serializeInDirectMemory(object.size(), pointer, offset + EMBEDDED_SIZE_OFFSET);

      long p = offset + EMBEDDED_VALUES_OFFSET;
      for (OIdentifiable ids : object) {
        LINK_SERIALIZER.serializeInDirectMemoryObject(ids, pointer, p);
        p += RID_SIZE;
      }
    } else {
      final OIndexRIDContainerSBTree underlying = (OIndexRIDContainerSBTree) object.getUnderlying();
      final OBonsaiBucketPointer rootPointer = underlying.getRootPointer();
      LONG_SERIALIZER.serializeInDirectMemory(rootPointer.getPageIndex(), pointer, offset + SBTREE_ROOTINDEX_OFFSET);
      INT_SERIALIZER.serializeInDirectMemory(rootPointer.getPageOffset(), pointer, offset + SBTREE_ROOTOFFSET_OFFSET);
    }
  }

  @Override
  public OIndexRIDContainer deserializeFromDirectMemoryObject(ODirectMemoryPointer pointer, long offset) {
    final long fileId = LONG_SERIALIZER.deserializeFromDirectMemory(pointer, offset + FILE_ID_OFFSET);
    if (BOOLEAN_SERIALIZER.deserializeFromDirectMemory(pointer, offset + EMBEDDED_OFFSET)) {
      final int size = INT_SERIALIZER.deserializeFromDirectMemory(pointer, offset + EMBEDDED_SIZE_OFFSET);
      final Set<OIdentifiable> underlying = new HashSet<OIdentifiable>(Math.max((int) (size / .75f) + 1, 16));

      long p = offset + EMBEDDED_VALUES_OFFSET;
      for (int i = 0; i < size; i++) {
        underlying.add(LINK_SERIALIZER.deserializeFromDirectMemoryObject(pointer, p));
        p += RID_SIZE;
      }

      return new OIndexRIDContainer(fileId, underlying, false);
    } else {
      final long pageIndex = LONG_SERIALIZER.deserializeFromDirectMemory(pointer, offset + SBTREE_ROOTINDEX_OFFSET);
      final int pageOffset = INT_SERIALIZER.deserializeFromDirectMemory(pointer, offset + SBTREE_ROOTOFFSET_OFFSET);
      final OBonsaiBucketPointer rootPointer = new OBonsaiBucketPointer(pageIndex, pageOffset);

      final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.get();
      final OIndexRIDContainerSBTree underlying = new OIndexRIDContainerSBTree(fileId, rootPointer, false,
          (OAbstractPaginatedStorage) db.getStorage());
      return new OIndexRIDContainer(fileId, underlying, false);
    }
  }

  @Override
  public OIndexRIDContainer deserializeFromDirectMemoryObject(OWALChangesTree.PointerWrapper wrapper, long offset) {
    final long fileId = LONG_SERIALIZER.deserializeFromDirectMemory(wrapper, offset + FILE_ID_OFFSET);
    if (BOOLEAN_SERIALIZER.deserializeFromDirectMemory(wrapper, offset + EMBEDDED_OFFSET)) {
      final int size = INT_SERIALIZER.deserializeFromDirectMemory(wrapper, offset + EMBEDDED_SIZE_OFFSET);
      final Set<OIdentifiable> underlying = new HashSet<OIdentifiable>(Math.max((int) (size / .75f) + 1, 16));

      long p = offset + EMBEDDED_VALUES_OFFSET;
      for (int i = 0; i < size; i++) {
        underlying.add(LINK_SERIALIZER.deserializeFromDirectMemoryObject(wrapper, p));
        p += RID_SIZE;
      }

      return new OIndexRIDContainer(fileId, underlying, false);
    } else {
      final long pageIndex = LONG_SERIALIZER.deserializeFromDirectMemory(wrapper, offset + SBTREE_ROOTINDEX_OFFSET);
      final int pageOffset = INT_SERIALIZER.deserializeFromDirectMemory(wrapper, offset + SBTREE_ROOTOFFSET_OFFSET);
      final OBonsaiBucketPointer rootPointer = new OBonsaiBucketPointer(pageIndex, pageOffset);
      final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.get();
      final OIndexRIDContainerSBTree underlying = new OIndexRIDContainerSBTree(fileId, rootPointer, false,
          (OAbstractPaginatedStorage) db.getStorage());
      return new OIndexRIDContainer(fileId, underlying, false);
    }
  }

  @Override
  public int getObjectSizeInDirectMemory(ODirectMemoryPointer pointer, long offset) {
    if (BOOLEAN_SERIALIZER.deserializeFromDirectMemory(pointer, offset + EMBEDDED_OFFSET)) {
      return embeddedObjectSerializedSize(INT_SERIALIZER.deserializeFromDirectMemory(pointer, offset + EMBEDDED_SIZE_OFFSET));
    } else {
      return SBTREE_CONTAINER_SIZE;
    }
  }

  @Override
  public int getObjectSizeInDirectMemory(OWALChangesTree.PointerWrapper wrapper, long offset) {
    if (BOOLEAN_SERIALIZER.deserializeFromDirectMemory(wrapper, offset + EMBEDDED_OFFSET)) {
      return embeddedObjectSerializedSize(INT_SERIALIZER.deserializeFromDirectMemory(wrapper, offset + EMBEDDED_SIZE_OFFSET));
    } else {
      return SBTREE_CONTAINER_SIZE;
    }
  }

  @Override
  public OIndexRIDContainer preprocess(OIndexRIDContainer value, Object... hints) {
    return value;
  }

  private int embeddedObjectSerializedSize(int size) {
    return OLongSerializer.LONG_SIZE + OBooleanSerializer.BOOLEAN_SIZE + OIntegerSerializer.INT_SIZE + size * RID_SIZE;
  }
}

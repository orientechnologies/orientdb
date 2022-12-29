/*
 * Copyright 2018 OrientDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OSharedContext;
import com.orientechnologies.orient.core.db.OStringCache;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.OTrackedMultiValue;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBagDelegate;
import com.orientechnologies.orient.core.db.record.ridbag.embedded.OEmbeddedRidBag;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OGlobalProperty;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.Change;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.ChangeSerializationHelper;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeRidBag;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

/** @author mdjurovi */
public class HelperClasses {
  public static final String CHARSET_UTF_8 = "UTF-8";
  protected static final ORecordId NULL_RECORD_ID = new ORecordId(-2, ORID.CLUSTER_POS_INVALID);
  public static final long MILLISEC_PER_DAY = 86400000;

  public static class Tuple<T1, T2> {

    private final T1 firstVal;
    private final T2 secondVal;

    Tuple(T1 firstVal, T2 secondVal) {
      this.firstVal = firstVal;
      this.secondVal = secondVal;
    }

    public T1 getFirstVal() {
      return firstVal;
    }

    public T2 getSecondVal() {
      return secondVal;
    }
  }

  protected static class RecordInfo {
    public int fieldStartOffset;
    public int fieldLength;
    public OType fieldType;
  }

  protected static class MapRecordInfo extends RecordInfo {
    public String key;
    public OType keyType;
  }

  public static OType readOType(final BytesContainer bytes, boolean justRunThrough) {
    if (justRunThrough) {
      bytes.offset++;
      return null;
    }
    return OType.getById(readByte(bytes));
  }

  public static void writeOType(BytesContainer bytes, int pos, OType type) {
    bytes.bytes[pos] = (byte) type.getId();
  }

  public static void writeType(BytesContainer bytes, OType type) {
    int pos = bytes.alloc(1);
    bytes.bytes[pos] = (byte) type.getId();
  }

  public static OType readType(BytesContainer bytes) {
    byte typeId = bytes.bytes[bytes.offset++];
    if (typeId == -1) {
      return null;
    }
    return OType.getById(typeId);
  }

  public static byte[] readBinary(final BytesContainer bytes) {
    final int n = OVarIntSerializer.readAsInteger(bytes);
    final byte[] newValue = new byte[n];
    System.arraycopy(bytes.bytes, bytes.offset, newValue, 0, newValue.length);
    bytes.skip(n);
    return newValue;
  }

  public static String readString(final BytesContainer bytes) {
    final int len = OVarIntSerializer.readAsInteger(bytes);
    if (len == 0) {
      return "";
    }
    final String res = stringFromBytes(bytes.bytes, bytes.offset, len);
    bytes.skip(len);
    return res;
  }

  public static int readInteger(final BytesContainer container) {
    final int value =
        OIntegerSerializer.INSTANCE.deserializeLiteral(container.bytes, container.offset);
    container.offset += OIntegerSerializer.INT_SIZE;
    return value;
  }

  public static byte readByte(final BytesContainer container) {
    return container.bytes[container.offset++];
  }

  public static long readLong(final BytesContainer container) {
    final long value =
        OLongSerializer.INSTANCE.deserializeLiteral(container.bytes, container.offset);
    container.offset += OLongSerializer.LONG_SIZE;
    return value;
  }

  public static ORecordId readOptimizedLink(final BytesContainer bytes, boolean justRunThrough) {
    int clusterId = OVarIntSerializer.readAsInteger(bytes);
    long clusterPos = OVarIntSerializer.readAsLong(bytes);
    if (justRunThrough) return null;
    else return new ORecordId(clusterId, clusterPos);
  }

  public static String stringFromBytes(final byte[] bytes, final int offset, final int len) {
    try {
      return new String(bytes, offset, len, CHARSET_UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw OException.wrapException(new OSerializationException("Error on string decoding"), e);
    }
  }

  public static String stringFromBytesIntern(final byte[] bytes, final int offset, final int len) {
    try {
      ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
      if (db != null) {
        OSharedContext context = db.getSharedContext();
        if (context != null) {
          OStringCache cache = context.getStringCache();
          if (cache != null) {
            return cache.getString(bytes, offset, len);
          }
        }
      }
      return new String(bytes, offset, len, CHARSET_UTF_8).intern();
    } catch (UnsupportedEncodingException e) {
      throw OException.wrapException(new OSerializationException("Error on string decoding"), e);
    }
  }

  public static byte[] bytesFromString(final String toWrite) {
    try {
      return toWrite.getBytes(CHARSET_UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw OException.wrapException(new OSerializationException("Error on string encoding"), e);
    }
  }

  public static long convertDayToTimezone(TimeZone from, TimeZone to, long time) {
    Calendar fromCalendar = Calendar.getInstance(from);
    fromCalendar.setTimeInMillis(time);
    Calendar toCalendar = Calendar.getInstance(to);
    toCalendar.setTimeInMillis(0);
    toCalendar.set(Calendar.ERA, fromCalendar.get(Calendar.ERA));
    toCalendar.set(Calendar.YEAR, fromCalendar.get(Calendar.YEAR));
    toCalendar.set(Calendar.MONTH, fromCalendar.get(Calendar.MONTH));
    toCalendar.set(Calendar.DAY_OF_MONTH, fromCalendar.get(Calendar.DAY_OF_MONTH));
    toCalendar.set(Calendar.HOUR_OF_DAY, 0);
    toCalendar.set(Calendar.MINUTE, 0);
    toCalendar.set(Calendar.SECOND, 0);
    toCalendar.set(Calendar.MILLISECOND, 0);
    return toCalendar.getTimeInMillis();
  }

  public static OGlobalProperty getGlobalProperty(final ODocument document, final int len) {
    final int id = (len * -1) - 1;
    return ODocumentInternal.getGlobalPropertyById(document, id);
  }

  public static int writeBinary(final BytesContainer bytes, final byte[] valueBytes) {
    final int pointer = OVarIntSerializer.write(bytes, valueBytes.length);
    final int start = bytes.alloc(valueBytes.length);
    System.arraycopy(valueBytes, 0, bytes.bytes, start, valueBytes.length);
    return pointer;
  }

  public static int writeOptimizedLink(final BytesContainer bytes, OIdentifiable link) {
    if (!link.getIdentity().isPersistent()) {
      try {
        final ORecord real = link.getRecord();
        if (real != null) link = real;
      } catch (ORecordNotFoundException ignored) {
        // IGNORE IT WILL FAIL THE ASSERT IN CASE
      }
    }
    if (link.getIdentity().getClusterId() < 0)
      throw new ODatabaseException("Impossible to serialize invalid link " + link.getIdentity());

    final int pos = OVarIntSerializer.write(bytes, link.getIdentity().getClusterId());
    OVarIntSerializer.write(bytes, link.getIdentity().getClusterPosition());
    return pos;
  }

  public static int writeNullLink(final BytesContainer bytes) {
    final int pos = OVarIntSerializer.write(bytes, NULL_RECORD_ID.getIdentity().getClusterId());
    OVarIntSerializer.write(bytes, NULL_RECORD_ID.getIdentity().getClusterPosition());
    return pos;
  }

  public static OType getTypeFromValueEmbedded(final Object fieldValue) {
    OType type = OType.getTypeByValue(fieldValue);
    if (type == OType.LINK
        && fieldValue instanceof ODocument
        && !((ODocument) fieldValue).getIdentity().isValid()) type = OType.EMBEDDED;
    return type;
  }

  public static int writeLinkCollection(
      final BytesContainer bytes, final Collection<OIdentifiable> value) {
    final int pos = OVarIntSerializer.write(bytes, value.size());

    final boolean disabledAutoConversion =
        value instanceof ORecordLazyMultiValue
            && ((ORecordLazyMultiValue) value).isAutoConvertToRecord();

    if (disabledAutoConversion)
      // AVOID TO FETCH RECORD
      ((ORecordLazyMultiValue) value).setAutoConvertToRecord(false);

    try {
      for (OIdentifiable itemValue : value) {
        // TODO: handle the null links
        if (itemValue == null) writeNullLink(bytes);
        else writeOptimizedLink(bytes, itemValue);
      }

    } finally {
      if (disabledAutoConversion) ((ORecordLazyMultiValue) value).setAutoConvertToRecord(true);
    }

    return pos;
  }

  public static <T extends OTrackedMultiValue<?, OIdentifiable>> T readLinkCollection(
      final BytesContainer bytes, final T found, boolean justRunThrough) {
    final int items = OVarIntSerializer.readAsInteger(bytes);
    for (int i = 0; i < items; i++) {
      ORecordId id = readOptimizedLink(bytes, justRunThrough);
      if (!justRunThrough) {
        if (id.equals(NULL_RECORD_ID)) found.addInternal(null);
        else found.addInternal(id);
      }
    }
    return found;
  }

  public static int writeString(final BytesContainer bytes, final String toWrite) {
    final byte[] nameBytes = bytesFromString(toWrite);
    final int pointer = OVarIntSerializer.write(bytes, nameBytes.length);
    final int start = bytes.alloc(nameBytes.length);
    System.arraycopy(nameBytes, 0, bytes.bytes, start, nameBytes.length);
    return pointer;
  }

  public static int writeLinkMap(final BytesContainer bytes, final Map<Object, OIdentifiable> map) {
    final boolean disabledAutoConversion =
        map instanceof ORecordLazyMultiValue
            && ((ORecordLazyMultiValue) map).isAutoConvertToRecord();

    if (disabledAutoConversion)
      // AVOID TO FETCH RECORD
      ((ORecordLazyMultiValue) map).setAutoConvertToRecord(false);

    try {
      final int fullPos = OVarIntSerializer.write(bytes, map.size());
      for (Map.Entry<Object, OIdentifiable> entry : map.entrySet()) {
        writeString(bytes, entry.getKey().toString());
        if (entry.getValue() == null) writeNullLink(bytes);
        else writeOptimizedLink(bytes, entry.getValue());
      }
      return fullPos;

    } finally {
      if (disabledAutoConversion) ((ORecordLazyMultiValue) map).setAutoConvertToRecord(true);
    }
  }

  public static Map<Object, OIdentifiable> readLinkMap(
      final BytesContainer bytes, final ORecordElement owner, boolean justRunThrough) {
    int size = OVarIntSerializer.readAsInteger(bytes);
    ORecordLazyMap result = null;
    if (!justRunThrough) result = new ORecordLazyMap(owner);
    while ((size--) > 0) {
      final String key = readString(bytes);
      final ORecordId value = readOptimizedLink(bytes, justRunThrough);
      if (value.equals(NULL_RECORD_ID)) result.putInternal(key, null);
      else result.putInternal(key, value);
    }
    return result;
  }

  public static void writeByte(BytesContainer bytes, byte val) {
    int pos = bytes.alloc(OByteSerializer.BYTE_SIZE);
    OByteSerializer.INSTANCE.serialize(val, bytes.bytes, pos);
  }

  public static void writeRidBag(BytesContainer bytes, ORidBag ridbag) {
    ridbag.checkAndConvert();

    UUID ownerUuid = ridbag.getTemporaryId();

    int positionOffset = bytes.offset;
    final OSBTreeCollectionManager sbTreeCollectionManager =
        ODatabaseRecordThreadLocal.instance().get().getSbTreeCollectionManager();
    UUID uuid = null;
    if (sbTreeCollectionManager != null) uuid = sbTreeCollectionManager.listenForChanges(ridbag);

    byte configByte = 0;
    if (ridbag.isEmbedded()) configByte |= 1;

    if (uuid != null) configByte |= 2;

    // alloc will move offset and do skip
    int posForWrite = bytes.alloc(OByteSerializer.BYTE_SIZE);
    OByteSerializer.INSTANCE.serialize(configByte, bytes.bytes, posForWrite);

    // removed serializing UUID

    if (ridbag.isEmbedded()) {
      writeEmbeddedRidbag(bytes, ridbag);
    } else {
      writeSBTreeRidbag(bytes, ridbag, ownerUuid);
    }
  }

  protected static void writeEmbeddedRidbag(BytesContainer bytes, ORidBag ridbag) {
    OVarIntSerializer.write(bytes, ridbag.size());
    Object[] entries = ((OEmbeddedRidBag) ridbag.getDelegate()).getEntries();
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    for (int i = 0; i < entries.length; i++) {
      Object entry = entries[i];
      if (entry instanceof OIdentifiable) {
        OIdentifiable itemValue = (OIdentifiable) entry;
        final ORID rid = itemValue.getIdentity();
        if (db != null
            && !db.isClosed()
            && db.getTransaction().isActive()
            && !itemValue.getIdentity().isPersistent()) {
          itemValue = db.getTransaction().getRecord(itemValue.getIdentity());
        }
        if (itemValue == null) {
          // should never happen
          String errorMessage = "Found null entry in ridbag with rid=" + rid;
          OSerializationException exc = new OSerializationException(errorMessage);
          OLogManager.instance().error(ORecordSerializerBinaryV1.class, errorMessage, null);
          throw exc;
        } else {
          entries[i] = itemValue.getIdentity();
          writeLinkOptimized(bytes, itemValue);
        }
      }
    }
  }

  protected static void writeSBTreeRidbag(BytesContainer bytes, ORidBag ridbag, UUID ownerUuid) {
    ((OSBTreeRidBag) ridbag.getDelegate()).applyNewEntries();

    OBonsaiCollectionPointer pointer = ridbag.getPointer();

    final ORecordSerializationContext context;
    boolean remoteMode = ODatabaseRecordThreadLocal.instance().get().isRemote();
    if (remoteMode) {
      context = null;
    } else context = ORecordSerializationContext.getContext();

    if (pointer == null && context != null) {
      final int clusterId = getHighLevelDocClusterId(ridbag);
      assert clusterId > -1;
      try {
        final OAbstractPaginatedStorage storage =
            (OAbstractPaginatedStorage) ODatabaseRecordThreadLocal.instance().get().getStorage();
        final OAtomicOperation atomicOperation =
            storage.getAtomicOperationsManager().getCurrentOperation();

        assert atomicOperation != null;
        pointer =
            ODatabaseRecordThreadLocal.instance()
                .get()
                .getSbTreeCollectionManager()
                .createSBTree(clusterId, atomicOperation, ownerUuid);
      } catch (IOException e) {
        throw OException.wrapException(
            new ODatabaseException("Error during creation of ridbag"), e);
      }
    }

    ((OSBTreeRidBag) ridbag.getDelegate()).setCollectionPointer(pointer);

    OVarIntSerializer.write(bytes, pointer.getFileId());
    OVarIntSerializer.write(bytes, pointer.getRootPointer().getPageIndex());
    OVarIntSerializer.write(bytes, pointer.getRootPointer().getPageOffset());
    OVarIntSerializer.write(bytes, 0);

    if (context != null) {
      ((OSBTreeRidBag) ridbag.getDelegate()).handleContextSBTree(context, pointer);
      OVarIntSerializer.write(bytes, 0);
    } else {
      OVarIntSerializer.write(bytes, 0);

      // removed changes serialization
    }
  }

  private static int getHighLevelDocClusterId(ORidBag ridbag) {
    ORidBagDelegate delegate = ridbag.getDelegate();
    ORecordElement owner = delegate.getOwner();
    while (owner != null && owner.getOwner() != null) {
      owner = owner.getOwner();
    }

    if (owner != null) return ((OIdentifiable) owner).getIdentity().getClusterId();

    return -1;
  }

  public static void writeLinkOptimized(final BytesContainer bytes, OIdentifiable link) {
    ORID id = link.getIdentity();
    OVarIntSerializer.write(bytes, id.getClusterId());
    OVarIntSerializer.write(bytes, id.getClusterPosition());
  }

  public static ORidBag readRidbag(BytesContainer bytes) {
    byte configByte = OByteSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset++);
    boolean isEmbedded = (configByte & 1) != 0;

    UUID uuid = null;
    // removed deserializing UUID

    ORidBag ridbag = null;
    if (isEmbedded) {
      ridbag = new ORidBag();
      int size = OVarIntSerializer.readAsInteger(bytes);
      ridbag.getDelegate().setSize(size);
      for (int i = 0; i < size; i++) {
        OIdentifiable record = readLinkOptimizedEmbedded(bytes);
        ((OEmbeddedRidBag) ridbag.getDelegate()).addInternal(record);
      }
    } else {
      long fileId = OVarIntSerializer.readAsLong(bytes);
      long pageIndex = OVarIntSerializer.readAsLong(bytes);
      int pageOffset = OVarIntSerializer.readAsInteger(bytes);
      // read bag size
      OVarIntSerializer.readAsInteger(bytes);

      OBonsaiCollectionPointer pointer = null;
      if (fileId != -1)
        pointer =
            new OBonsaiCollectionPointer(fileId, new OBonsaiBucketPointer(pageIndex, pageOffset));

      Map<OIdentifiable, Change> changes = new HashMap<>();

      int changesSize = OVarIntSerializer.readAsInteger(bytes);
      for (int i = 0; i < changesSize; i++) {
        OIdentifiable recId = readLinkOptimizedSBTree(bytes);
        Change change = deserializeChange(bytes);
        changes.put(recId, change);
      }

      ridbag = new ORidBag(pointer, changes, uuid);
      ridbag.getDelegate().setSize(-1);
    }
    return ridbag;
  }

  private static OIdentifiable readLinkOptimizedEmbedded(final BytesContainer bytes) {
    ORID rid =
        new ORecordId(OVarIntSerializer.readAsInteger(bytes), OVarIntSerializer.readAsLong(bytes));
    OIdentifiable identifiable = null;
    if (rid.isTemporary()) identifiable = rid.getRecord();

    if (identifiable == null) identifiable = rid;

    return identifiable;
  }

  private static OIdentifiable readLinkOptimizedSBTree(final BytesContainer bytes) {
    ORID rid =
        new ORecordId(OVarIntSerializer.readAsInteger(bytes), OVarIntSerializer.readAsLong(bytes));
    final OIdentifiable identifiable;
    if (rid.isTemporary() && rid.getRecord() != null) identifiable = rid.getRecord();
    else identifiable = rid;
    return identifiable;
  }

  private static Change deserializeChange(BytesContainer bytes) {
    byte type = OByteSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
    bytes.skip(OByteSerializer.BYTE_SIZE);
    int change = OIntegerSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
    bytes.skip(OIntegerSerializer.INT_SIZE);
    return ChangeSerializationHelper.createChangeInstance(type, change);
  }

  public static OType getLinkedType(OClass clazz, OType type, String key) {
    if (type != OType.EMBEDDEDLIST && type != OType.EMBEDDEDSET && type != OType.EMBEDDEDMAP)
      return null;
    if (clazz != null) {
      OProperty prop = clazz.getProperty(key);
      if (prop != null) {
        return prop.getLinkedType();
      }
    }
    return null;
  }
}

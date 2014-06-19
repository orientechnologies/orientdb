/*
 * Copyright 2010-2012 Luca Garulli (l.garulli(at)orientechnologies.com)
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

package com.orientechnologies.orient.core.db.record.ridbag;

import com.orientechnologies.common.collection.OCollection;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OUUIDSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeListener;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.OTrackedMultiValue;
import com.orientechnologies.orient.core.db.record.ridbag.embedded.OEmbeddedRidBag;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeRidBag;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringBuilderSerializable;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * A collection that contain links to {@link OIdentifiable}. Bag is similar to set but can contain several entering of the same
 * object.<br/>
 * 
 * Could be tree based and embedded representation.<br/>
 * <ul>
 * <li>
 * <b>Embedded</b> stores its content directly to the document that owns it.<br/>
 * It better fits for cases when only small amount of links are stored to the bag.<br/>
 * </li>
 * <li>
 * <b>Tree-based</b> implementation stores its content in a separate data structure called {@link OSBTreeBonsai}.<br/>
 * It fits great for cases when you have a huge amount of links.<br/>
 * </li>
 * </ul>
 * <br/>
 * The representation is automatically converted to tree-based implementation when top threshold is reached. And backward to
 * embedded one when size is decreased to bottom threshold. <br/>
 * The thresholds could be configured by {@link OGlobalConfiguration#RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD} and
 * {@link OGlobalConfiguration#RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD}. <br/>
 * <br/>
 * This collection is used to efficiently manage relationships in graph model.<br/>
 * <br/>
 * Does not implement {@link Collection} interface because some operations could not be efficiently implemented and that's why
 * should be avoided.<br/>
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 * @author Andrey Lomakin
 * @since 1.7rc1
 */
public class ORidBag implements OStringBuilderSerializable, Iterable<OIdentifiable>, ORecordLazyMultiValue,
    OTrackedMultiValue<OIdentifiable, OIdentifiable>, OCollection<OIdentifiable> {
  private ORidBagDelegate delegate;

  private int             topThreshold    = OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
  private int             bottomThreshold = OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();

  private UUID            uuid;

  public ORidBag(ORidBag ridBag) {
    this();

    for (OIdentifiable identifiable : ridBag)
      add(identifiable);
  }

  public ORidBag() {
    if (topThreshold < 0)
      delegate = new OSBTreeRidBag();
    else
      delegate = new OEmbeddedRidBag();
  }

  private ORidBag(byte[] stream) {
    fromStream(stream);
  }

  public void addAll(Collection<OIdentifiable> values) {
    delegate.addAll(values);
  }

  public void add(OIdentifiable identifiable) {
    delegate.add(identifiable);
  }

  public void remove(OIdentifiable identifiable) {
    delegate.remove(identifiable);
  }

  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  public Iterator<OIdentifiable> iterator() {
    return delegate.iterator();
  }

  @Override
  public Iterator<OIdentifiable> rawIterator() {
    return delegate.rawIterator();
  }

  @Override
  public void convertLinks2Records() {
    delegate.convertLinks2Records();
  }

  @Override
  public boolean convertRecords2Links() {
    return delegate.convertRecords2Links();
  }

  @Override
  public boolean isAutoConvertToRecord() {
    return delegate.isAutoConvertToRecord();
  }

  @Override
  public void setAutoConvertToRecord(boolean convertToRecord) {
    delegate.setAutoConvertToRecord(convertToRecord);
  }

  @Override
  public boolean detach() {
    return delegate.detach();
  }

  @Override
  public int size() {
    return delegate.size();
  }

  public boolean isEmbedded() {
    return delegate instanceof OEmbeddedRidBag;
  }

  public int toStream(BytesContainer bytesContainer) throws OSerializationException {

    final ORecordSerializationContext context = ORecordSerializationContext.getContext();
    if (context != null) {
      if (delegate.size() >= topThreshold && isEmbedded()
          && ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager() != null) {
        ORidBagDelegate oldDelegate = delegate;
        delegate = new OSBTreeRidBag();
        boolean oldAutoConvert = oldDelegate.isAutoConvertToRecord();
        oldDelegate.setAutoConvertToRecord(false);

        for (OIdentifiable identifiable : oldDelegate)
          delegate.add(identifiable);

        delegate.setOwner(oldDelegate.getOwner());
        oldDelegate.setAutoConvertToRecord(oldAutoConvert);
        oldDelegate.requestDelete();

      } else if (delegate.size() <= bottomThreshold && !isEmbedded()) {
        ORidBagDelegate oldDelegate = delegate;
        boolean oldAutoConvert = oldDelegate.isAutoConvertToRecord();
        oldDelegate.setAutoConvertToRecord(false);
        delegate = new OEmbeddedRidBag();

        for (OIdentifiable identifiable : oldDelegate)
          delegate.add(identifiable);

        delegate.setOwner(oldDelegate.getOwner());
        oldDelegate.setAutoConvertToRecord(oldAutoConvert);
        oldDelegate.requestDelete();
      }
    }

    final UUID oldUuid = uuid;
    final OSBTreeCollectionManager sbTreeCollectionManager = ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager();
    if (sbTreeCollectionManager != null)
      uuid = sbTreeCollectionManager.listenForChanges(this);
    else
      uuid = null;

    boolean hasUuid = uuid != null;

    final int serializedSize = OByteSerializer.BYTE_SIZE + delegate.getSerializedSize()
        + ((hasUuid) ? OUUIDSerializer.UUID_SIZE : 0);
    int pointer = bytesContainer.alloc((short) serializedSize);
    int offset = pointer;
    final byte[] stream = bytesContainer.bytes;

    byte configByte = 0;
    if (isEmbedded())
      configByte |= 1;

    if (hasUuid)
      configByte |= 2;

    stream[offset++] = configByte;

    if (hasUuid) {
      OUUIDSerializer.INSTANCE.serialize(uuid, stream, offset);
      offset += OUUIDSerializer.UUID_SIZE;
    }

    delegate.serialize(stream, offset, oldUuid);
    return pointer;
  }

  @Override
  public OStringBuilderSerializable toStream(StringBuilder output) throws OSerializationException {
    BytesContainer container = new BytesContainer();
    toStream(container);
    output.append(OBase64Utils.encodeBytes(container.bytes, 0, container.offset));
    return this;
  }

  @Override
  public String toString() {
    return delegate.toString();
  }

  public void delete() {
    delegate.requestDelete();
  }

  @Override
  public OStringBuilderSerializable fromStream(StringBuilder input) throws OSerializationException {
    final byte[] stream = OBase64Utils.decode(input.toString());
    fromStream(stream);
    return this;
  }

  public void fromStream(byte[] stream) {
    fromStream(new BytesContainer(stream));
  }

  public void fromStream(BytesContainer stream) {
    byte first = stream.bytes[stream.offset++];
    if ((first & 1) == 1)
      delegate = new OEmbeddedRidBag();
    else
      delegate = new OSBTreeRidBag();

    if ((first & 2) == 2) {
      uuid = OUUIDSerializer.INSTANCE.deserialize(stream.bytes, stream.offset);
      stream.read(OUUIDSerializer.UUID_SIZE);
    }

    stream.read(delegate.deserialize(stream.bytes, stream.offset));
  }

  public static ORidBag fromStream(String value) {
    final byte[] stream = OBase64Utils.decode(value);
    return new ORidBag(stream);
  }

  @Override
  public void addChangeListener(OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener) {
    delegate.addChangeListener(changeListener);
  }

  @Override
  public void removeRecordChangeListener(OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener) {
    delegate.removeRecordChangeListener(changeListener);
  }

  @Override
  public Object returnOriginalState(List<OMultiValueChangeEvent<OIdentifiable, OIdentifiable>> multiValueChangeEvents) {
    return delegate.returnOriginalState(multiValueChangeEvents);
  }

  @Override
  public Class<?> getGenericClass() {
    return delegate.getGenericClass();
  }

  public void setOwner(ORecord<?> owner) {
    delegate.setOwner(owner);
  }

  /**
   * Temporary id of collection to track changes in remote mode.
   * 
   * WARNING! Method is for internal usage.
   * 
   * @return UUID
   */
  public UUID getTemporaryId() {
    return uuid;
  }

  /**
   * Notify collection that changes has been saved. Converts to non embedded implementation if needed.
   * 
   * WARNING! Method is for internal usage.
   * 
   * @param newPointer
   *          new collection pointer
   */
  public void notifySaved(OBonsaiCollectionPointer newPointer) {
    if (newPointer.isValid()) {
      if (isEmbedded()) {
        replaceWithSBTree(newPointer);
      } else {
        ((OSBTreeRidBag) delegate).setCollectionPointer(newPointer);
        ((OSBTreeRidBag) delegate).clearChanges();
      }
    }
  }

  /**
   * Silently replace delegate by tree implementation.
   * 
   * @param pointer
   *          new collection pointer
   */
  private void replaceWithSBTree(OBonsaiCollectionPointer pointer) {
    delegate.requestDelete();
    final OSBTreeRidBag treeBag = new OSBTreeRidBag();
    treeBag.setCollectionPointer(pointer);
    delegate = treeBag;
  }

  public OBonsaiCollectionPointer getPointer() {
    if (isEmbedded()) {
      return OBonsaiCollectionPointer.INVALID;
    } else {
      return ((OSBTreeRidBag) delegate).getCollectionPointer();
    }
  }
}

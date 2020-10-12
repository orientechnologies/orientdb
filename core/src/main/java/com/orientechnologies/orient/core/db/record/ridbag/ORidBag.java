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

package com.orientechnologies.orient.core.db.record.ridbag;

import com.orientechnologies.common.collection.OCollection;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OUUIDSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeTimeLine;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.OTrackedMultiValue;
import com.orientechnologies.orient.core.db.record.ridbag.embedded.OEmbeddedRidBag;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringBuilderSerializable;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.storage.ridbag.ORemoteTreeRidBag;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.Change;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeRidBag;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Base64;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.UUID;

/**
 * A collection that contain links to {@link OIdentifiable}. Bag is similar to set but can contain
 * several entering of the same object.<br>
 *
 * <p>Could be tree based and embedded representation.<br>
 *
 * <ul>
 *   <li><b>Embedded</b> stores its content directly to the document that owns it.<br>
 *       It better fits for cases when only small amount of links are stored to the bag.<br>
 *   <li><b>Tree-based</b> implementation stores its content in a separate data structure called
 *       {@link OSBTreeBonsai}.<br>
 *       It fits great for cases when you have a huge amount of links.<br>
 * </ul>
 *
 * <br>
 * The representation is automatically converted to tree-based implementation when top threshold is
 * reached. And backward to embedded one when size is decreased to bottom threshold. <br>
 * The thresholds could be configured by {@link
 * OGlobalConfiguration#RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD} and {@link
 * OGlobalConfiguration#RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD}. <br>
 * <br>
 * This collection is used to efficiently manage relationships in graph model.<br>
 * <br>
 * Does not implement {@link Collection} interface because some operations could not be efficiently
 * implemented and that's why should be avoided.<br>
 *
 * @author Artem Orobets (enisher-at-gmail.com)
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 1.7rc1
 */
public class ORidBag
    implements OStringBuilderSerializable,
        Iterable<OIdentifiable>,
        ORecordLazyMultiValue,
        OTrackedMultiValue<OIdentifiable, OIdentifiable>,
        OCollection<OIdentifiable>,
        ORecordElement {
  private ORidBagDelegate delegate;
  private ORecordId ownerRecord;
  private String fieldName;

  private int topThreshold =
      OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
  private int bottomThreshold =
      OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();

  private UUID uuid;

  public ORidBag(final ORidBag ridBag) {
    init();
    for (Iterator<OIdentifiable> it = ridBag.rawIterator(); it.hasNext(); ) {
      add(it.next());
    }
  }

  public ORidBag() {
    init();
  }

  public ORidBag(final int iTopThreshold, final int iBottomThreshold) {
    topThreshold = iTopThreshold;
    bottomThreshold = iBottomThreshold;
    init();
  }

  public ORidBag(UUID uuid) {
    init();
    this.uuid = uuid;
  }

  public ORidBag(OBonsaiCollectionPointer pointer, Map<OIdentifiable, Change> changes, UUID uuid) {
    delegate = new OSBTreeRidBag(pointer, changes);
    this.uuid = uuid;
  }

  private ORidBag(final byte[] stream) {
    fromStream(stream);
  }

  public ORidBag(ORidBagDelegate delegate) {
    this.delegate = delegate;
  }

  public static ORidBag fromStream(final String value) {
    final byte[] stream = Base64.getDecoder().decode(value);
    return new ORidBag(stream);
  }

  public ORidBag copy() {
    final ORidBag copy = new ORidBag();
    copy.topThreshold = topThreshold;
    copy.bottomThreshold = bottomThreshold;
    copy.uuid = uuid;

    if (delegate instanceof OSBTreeRidBag)
    // ALREADY MULTI-THREAD
    {
      copy.delegate = delegate;
    } else {
      copy.delegate = ((OEmbeddedRidBag) delegate).copy();
    }

    return copy;
  }

  /**
   * THIS IS VERY EXPENSIVE METHOD AND CAN NOT BE CALLED IN REMOTE STORAGE.
   *
   * @param identifiable Object to check.
   * @return true if ridbag contains at leas one instance with the same rid as passed in
   *     identifiable.
   */
  public boolean contains(OIdentifiable identifiable) {
    return delegate.contains(identifiable);
  }

  public void addAll(Collection<OIdentifiable> values) {
    delegate.addAll(values);
  }

  @Override
  public void add(OIdentifiable identifiable) {
    delegate.add(identifiable);
  }

  @Override
  public boolean addInternal(OIdentifiable e) {
    return delegate.addInternal(e);
  }

  @Override
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

  public boolean isToSerializeEmbedded() {
    if (isEmbedded()) return true;
    if (getOwner() instanceof ORecord && !((ORecord) getOwner()).getIdentity().isPersistent()) {
      return true;
    }
    if (OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger()
        >= size()) {
      return true;
    }
    return false;
  }

  public int toStream(BytesContainer bytesContainer) throws OSerializationException {

    checkAndConvert();

    final UUID oldUuid = uuid;
    final OSBTreeCollectionManager sbTreeCollectionManager =
        ODatabaseRecordThreadLocal.instance().get().getSbTreeCollectionManager();
    if (sbTreeCollectionManager != null) {
      uuid = sbTreeCollectionManager.listenForChanges(this);
    } else {
      uuid = null;
    }

    boolean hasUuid = uuid != null;

    final int serializedSize =
        OByteSerializer.BYTE_SIZE
            + delegate.getSerializedSize()
            + ((hasUuid) ? OUUIDSerializer.UUID_SIZE : 0);
    int pointer = bytesContainer.alloc(serializedSize);
    int offset = pointer;
    final byte[] stream = bytesContainer.bytes;

    byte configByte = 0;
    if (isEmbedded()) {
      configByte |= 1;
    }

    if (hasUuid) {
      configByte |= 2;
    }

    stream[offset++] = configByte;

    if (hasUuid) {
      OUUIDSerializer.INSTANCE.serialize(uuid, stream, offset);
      offset += OUUIDSerializer.UUID_SIZE;
    }

    delegate.serialize(stream, offset, oldUuid);
    return pointer;
  }

  public void checkAndConvert() {
    ODatabaseDocumentInternal database = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (database != null && !database.isRemote()) {
      if (isEmbedded()
          && ODatabaseRecordThreadLocal.instance().get().getSbTreeCollectionManager() != null
          && delegate.size() >= topThreshold) {
        convertToTree();
      } else if (bottomThreshold >= 0 && !isEmbedded() && delegate.size() <= bottomThreshold) {
        convertToEmbedded();
      }
    }
  }

  private void convertToEmbedded() {
    ORidBagDelegate oldDelegate = delegate;
    boolean isTransactionModified = oldDelegate.isTransactionModified();
    boolean oldAutoConvert = oldDelegate.isAutoConvertToRecord();
    oldDelegate.setAutoConvertToRecord(false);
    delegate = new OEmbeddedRidBag();

    final ORecordElement owner = oldDelegate.getOwner();
    delegate.disableTracking(owner);
    for (OIdentifiable identifiable : oldDelegate) {
      delegate.add(identifiable);
    }

    delegate.setOwner(owner);

    delegate.setTracker(oldDelegate.getTracker());
    oldDelegate.disableTracking(owner);

    delegate.setDirty();
    delegate.setTransactionModified(isTransactionModified);
    delegate.enableTracking(owner);

    oldDelegate.setAutoConvertToRecord(oldAutoConvert);
    oldDelegate.requestDelete();
  }

  private void convertToTree() {
    ORidBagDelegate oldDelegate = delegate;
    boolean isTransactionModified = oldDelegate.isTransactionModified();
    delegate = new OSBTreeRidBag();
    boolean oldAutoConvert = oldDelegate.isAutoConvertToRecord();
    oldDelegate.setAutoConvertToRecord(false);

    final ORecordElement owner = oldDelegate.getOwner();
    delegate.disableTracking(owner);
    for (OIdentifiable identifiable : oldDelegate) {
      delegate.add(identifiable);
    }

    delegate.setOwner(owner);

    delegate.setTracker(oldDelegate.getTracker());
    oldDelegate.disableTracking(owner);
    delegate.setDirty();
    delegate.setTransactionModified(isTransactionModified);
    delegate.enableTracking(owner);

    oldDelegate.setAutoConvertToRecord(oldAutoConvert);
    oldDelegate.requestDelete();
  }

  @Override
  public OStringBuilderSerializable toStream(StringBuilder output) throws OSerializationException {
    final BytesContainer container = new BytesContainer();
    toStream(container);
    output.append(Base64.getEncoder().encodeToString(container.fitBytes()));
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
    final byte[] stream = Base64.getDecoder().decode(input.toString());
    fromStream(stream);
    return this;
  }

  public void fromStream(final byte[] stream) {
    fromStream(new BytesContainer(stream));
  }

  public void fromStream(BytesContainer stream) {
    final byte first = stream.bytes[stream.offset++];
    if ((first & 1) == 1) {
      delegate = new OEmbeddedRidBag();
    } else {
      delegate = new OSBTreeRidBag();
    }

    if ((first & 2) == 2) {
      uuid = OUUIDSerializer.INSTANCE.deserialize(stream.bytes, stream.offset);
      stream.skip(OUUIDSerializer.UUID_SIZE);
    }

    stream.skip(delegate.deserialize(stream.bytes, stream.offset) - stream.offset);
  }

  @Override
  public Object returnOriginalState(
      List<OMultiValueChangeEvent<OIdentifiable, OIdentifiable>> multiValueChangeEvents) {
    return delegate.returnOriginalState(multiValueChangeEvents);
  }

  @Override
  public Class<?> getGenericClass() {
    return delegate.getGenericClass();
  }

  public void setOwner(ORecordElement owner) {
    if ((!(owner instanceof ODocument) && owner != null)
        || (owner != null && ((ODocument) owner).isEmbedded())) {
      throw new ODatabaseException("RidBag are supported only at document root");
    }
    delegate.setOwner(owner);
  }

  /**
   * Temporary id of collection to track changes in remote mode.
   *
   * <p>WARNING! Method is for internal usage.
   *
   * @return UUID
   */
  public UUID getTemporaryId() {
    return uuid;
  }

  public void setTemporaryId(UUID uuid) {
    this.uuid = uuid;
  }

  /**
   * Notify collection that changes has been saved. Converts to non embedded implementation if
   * needed.
   *
   * <p>WARNING! Method is for internal usage.
   *
   * @param newPointer new collection pointer
   */
  public void notifySaved(OBonsaiCollectionPointer newPointer) {
    if (newPointer.isValid()) {
      if (isEmbedded()) {
        replaceWithSBTree(newPointer);
      } else if (getDelegate() instanceof OSBTreeRidBag) {
        ((OSBTreeRidBag) delegate).setCollectionPointer(newPointer);
        ((OSBTreeRidBag) delegate).clearChanges();
      }
    }
  }

  public OBonsaiCollectionPointer getPointer() {
    if (isEmbedded()) {
      return OBonsaiCollectionPointer.INVALID;
    } else if (delegate instanceof ORemoteTreeRidBag) {
      return ((ORemoteTreeRidBag) delegate).getCollectionPointer();
    } else {
      return ((OSBTreeRidBag) delegate).getCollectionPointer();
    }
  }

  /** IMPORTANT! Only for internal usage. */
  public boolean tryMerge(final ORidBag otherValue, boolean iMergeSingleItemsOfMultiValueFields) {
    if (!isEmbedded() && !otherValue.isEmbedded()) {
      final OSBTreeRidBag thisTree = (OSBTreeRidBag) delegate;
      final OSBTreeRidBag otherTree = (OSBTreeRidBag) otherValue.delegate;
      if (thisTree.getCollectionPointer().equals(otherTree.getCollectionPointer())) {

        thisTree.mergeChanges(otherTree);

        uuid = otherValue.uuid;

        return true;
      }
    } else if (iMergeSingleItemsOfMultiValueFields) {
      final Iterator<OIdentifiable> iter = otherValue.rawIterator();
      while (iter.hasNext()) {
        final OIdentifiable value = iter.next();
        if (value != null) {
          final Iterator<OIdentifiable> localIter = rawIterator();
          boolean found = false;
          while (localIter.hasNext()) {
            final OIdentifiable v = localIter.next();
            if (value.equals(v)) {
              found = true;
              break;
            }
          }
          if (!found) {
            add(value);
          }
        }
      }
      return true;
    }
    return false;
  }

  protected void init() {
    if (topThreshold < 0) {
      if (ODatabaseRecordThreadLocal.instance().isDefined()
          && !ODatabaseRecordThreadLocal.instance().get().isRemote()) {
        delegate = new OSBTreeRidBag();
      } else {
        delegate = new OEmbeddedRidBag();
      }
    } else {
      delegate = new OEmbeddedRidBag();
    }
  }

  /**
   * Silently replace delegate by tree implementation.
   *
   * @param pointer new collection pointer
   */
  private void replaceWithSBTree(OBonsaiCollectionPointer pointer) {
    delegate.requestDelete();
    final ORemoteTreeRidBag treeBag = new ORemoteTreeRidBag(pointer);
    treeBag.setRecordAndField(ownerRecord, fieldName);
    treeBag.setOwner(delegate.getOwner());
    treeBag.setTracker(delegate.getTracker());
    delegate = treeBag;
  }

  public void debugPrint(PrintStream writer) throws IOException {
    if (delegate instanceof OSBTreeRidBag) {
      writer.append("tree [\n");
      ((OSBTreeRidBag) delegate).debugPrint(writer);
      writer.append("]\n");
    } else {
      writer.append(delegate.toString());
      writer.append("\n");
    }
  }

  public ORidBagDelegate getDelegate() {
    return delegate;
  }

  public NavigableMap<OIdentifiable, Change> getChanges() {
    return delegate.getChanges();
  }

  @Override
  public void replace(OMultiValueChangeEvent<Object, Object> event, Object newValue) {
    // not needed do nothing
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ORidBag)) {
      return false;
    }

    ORidBag otherRidbag = (ORidBag) other;
    if (!delegate.getClass().equals(otherRidbag.delegate.getClass())) {
      return false;
    }

    Iterator<OIdentifiable> firstIter = delegate.rawIterator();
    Iterator<OIdentifiable> secondIter = otherRidbag.delegate.rawIterator();
    while (firstIter.hasNext()) {
      if (!secondIter.hasNext()) {
        return false;
      }

      OIdentifiable firstElement = firstIter.next();
      OIdentifiable secondElement = secondIter.next();
      if (!Objects.equals(firstElement, secondElement)) {
        return false;
      }
    }
    if (secondIter.hasNext()) {
      return false;
    }

    return true;
  }

  @Override
  public void enableTracking(ORecordElement parent) {
    delegate.enableTracking(parent);
  }

  public void disableTracking(ORecordElement document) {
    delegate.disableTracking(document);
  }

  @Override
  public void transactionClear() {
    delegate.transactionClear();
  }

  @Override
  public boolean isModified() {
    return delegate.isModified();
  }

  @Override
  public OMultiValueChangeTimeLine<Object, Object> getTimeLine() {
    return delegate.getTimeLine();
  }

  @Override
  public <RET> RET setDirty() {
    return delegate.setDirty();
  }

  @Override
  public void setDirtyNoChanged() {
    delegate.setDirtyNoChanged();
  }

  @Override
  public ORecordElement getOwner() {
    return delegate.getOwner();
  }

  @Override
  public boolean isTransactionModified() {
    return delegate.isTransactionModified();
  }

  @Override
  public OMultiValueChangeTimeLine<OIdentifiable, OIdentifiable> getTransactionTimeLine() {
    return delegate.getTransactionTimeLine();
  }

  public void setRecordAndField(ORecordId id, String fieldName) {
    if (this.getDelegate() instanceof ORemoteTreeRidBag) {
      ((ORemoteTreeRidBag) this.getDelegate()).setRecordAndField(id, fieldName);
    }
    this.ownerRecord = id;
    this.fieldName = fieldName;
  }

  public void makeTree() {
    if (isEmbedded()) {
      convertToTree();
    }
  }

  public void makeEmbedded() {
    if (!isEmbedded()) {
      convertToEmbedded();
    }
  }
}

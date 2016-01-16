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
package com.orientechnologies.orient.core.type.tree;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;

import com.orientechnologies.common.collection.OLazyIterator;
import com.orientechnologies.orient.core.db.record.ODetachable;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeListener;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.OTrackedMultiValue;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringBuilderSerializable;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeRIDProvider;

/**
 * Persistent Set<OIdentifiable> implementation that uses the MVRB-Tree to handle entries in persistent way.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OMVRBTreeRIDSet implements Set<OIdentifiable>, OTrackedMultiValue<OIdentifiable, OIdentifiable>,
    ORecordLazyMultiValue, OStringBuilderSerializable, OSerializableStream, ODetachable {
  private static final long  serialVersionUID = 1L;

  private final OMVRBTreeRID tree;

  public OMVRBTreeRIDSet() {
    this(new OMVRBTreeRID());
  }

  public OMVRBTreeRIDSet(int binaryThreshold) {
    this(new OMVRBTreeRID(binaryThreshold));
  }

  public OMVRBTreeRIDSet(final OIdentifiable iRecord) {
    this(new OMVRBTreeRID((ODocument) iRecord.getRecord()));
  }

  public OMVRBTreeRIDSet(final String iClusterName) {
    this(new OMVRBTreeRID(iClusterName));
  }

  public OMVRBTreeRIDSet(final ORecord iOwner) {
    this((OMVRBTreeRID) new OMVRBTreeRID().setOwner(iOwner));
  }

  public OMVRBTreeRIDSet(final ORecord iOwner, final Collection<OIdentifiable> iInitValues) {
    this((OMVRBTreeRID) new OMVRBTreeRID(iInitValues).setOwner(iOwner));
  }

  public OMVRBTreeRIDSet(final OMVRBTreeRID iProvider) {
    tree = iProvider;
  }

  /**
   * Copy constructor
   * 
   * @param iSource
   *          Source object
   */
  public OMVRBTreeRIDSet(final OMVRBTreeRIDSet iSource, final ODocument iClone) {
    tree = new OMVRBTreeRID(iSource.tree);
    tree.setOwner(iClone);
  }

  @Override
  public int size() {
    return tree.size();
  }

  public boolean isEmpty() {
    return tree.isEmpty();
  }

  public boolean contains(final Object o) {
    return tree.containsKey(o);
  }

  public OLazyIterator<OIdentifiable> iterator(final boolean iAutoConvertToRecord) {
    return tree.iterator(iAutoConvertToRecord);
  }

  public Iterator<OIdentifiable> iterator() {
    return tree.iterator();
  }

  public Object[] toArray() {
    return tree.toArray();
  }

  public <T> T[] toArray(final T[] a) {
    return tree.toArray(a);
  }

  public boolean add(final OIdentifiable e) {
    return tree.put(e, null) == null;
  }

  public boolean remove(final Object o) {
    if (o == null)
      return clearDeletedRecords();

    return tree.remove(o) != null;
  }

  public boolean clearDeletedRecords() {
    boolean removed = false;
    Iterator<OIdentifiable> all = tree.iterator();
    while (all.hasNext()) {
      OIdentifiable entry = all.next();
      try {
        if (entry == null || entry.getRecord() == null) {
          all.remove();
          removed = true;
        }
      } catch (ORecordNotFoundException e) {
        all.remove();
        removed = true;
      }
    }
    return removed;
  }

  public boolean containsAll(final Collection<?> c) {
    for (Object o : c)
      if (!tree.containsKey(o))
        return false;
    return true;
  }

  public boolean addAll(final Collection<? extends OIdentifiable> c) {
    boolean changed = false;
    for (OIdentifiable o : c)
      if (add(o) && !changed)
        changed = true;
    return changed;
  }

  public boolean retainAll(final Collection<?> c) {
    return tree.retainAll(c);
  }

  public boolean removeAll(final Collection<?> c) {
    return tree.removeAll(c);
  }

  public boolean detach() {
    return tree.detach();
  }

  public void clear() {
    tree.clear();
  }

  public OMVRBTreeRIDSet fromDocument(final ODocument iDocument) {
    fromStream(iDocument.toStream());
    return this;
  }

  public ODocument toDocument() {
    return ((OMVRBTreeRIDProvider) tree.getProvider()).toDocument();
  }

  public OMVRBTreeRIDSet copy(final ODocument iCloned) {
    final OMVRBTreeRIDSet clone = new OMVRBTreeRIDSet(this, iCloned);
    return clone;
  }

  public OStringBuilderSerializable fromStream(final StringBuilder iSource) {
    ((OMVRBTreeRIDProvider) tree.getProvider()).fromStream(iSource);
    return this;
  }

  public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
    fromStream(new StringBuilder(OBinaryProtocol.bytes2string(iStream)));
    return this;
  }

  public OStringBuilderSerializable toStream(StringBuilder iOutput) throws OSerializationException {
    ((OMVRBTreeRIDProvider) tree.getProvider()).toStream(iOutput);
    return this;
  }

  public byte[] toStream() throws OSerializationException {
    final StringBuilder buffer = new StringBuilder(128);
    toStream(buffer);
    return buffer.toString().getBytes();
  }

  @Override
  public String toString() {
    return tree.toString();
  }

  @Override
  public void addChangeListener(OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener) {
    tree.addChangeListener(changeListener);
  }

  @Override
  public void removeRecordChangeListener(OMultiValueChangeListener<OIdentifiable, OIdentifiable> changeListener) {
    tree.removeRecordChangeListener(changeListener);
  }

  @Override
  public Object returnOriginalState(List<OMultiValueChangeEvent<OIdentifiable, OIdentifiable>> changeEvents) {
    return tree.returnOriginalState(changeEvents);
  }

  @Override
  public Class<?> getGenericClass() {
    return tree.getGenericClass();
  }

  @Override
  public Iterator<OIdentifiable> rawIterator() {
    return tree.rawIterator();
  }

  @Override
  public void convertLinks2Records() {
    tree.convertLinks2Records();
  }

  @Override
  public boolean convertRecords2Links() {
    return tree.convertRecords2Links();
  }

  @Override
  public boolean isAutoConvertToRecord() {
    return tree.isAutoConvertToRecord();
  }

  @Override
  public void setAutoConvertToRecord(final boolean convertToRecord) {
    tree.setAutoConvertToRecord(convertToRecord);
  }
}

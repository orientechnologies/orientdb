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
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.*;
import com.orientechnologies.orient.core.db.record.ridbag.embedded.OEmbeddedRidBag;
import com.orientechnologies.orient.core.db.record.ridbag.sbtree.OSBTreeRidBag;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.OBase64Utils;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringBuilderSerializable;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ORecordSerializationContext;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class ORidBag implements OStringBuilderSerializable, Iterable<OIdentifiable>, ORecordLazyMultiValue,
    OTrackedMultiValue<OIdentifiable, OIdentifiable>, OCollection<OIdentifiable> {
  private ORidBagDelegate delegate;

  private int             topThreshold    = OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
  private int             bottomThreshold = OGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();

  public ORidBag() {
    if (topThreshold < 0)
      delegate = new OSBTreeRidBag();
    else
      delegate = new OEmbeddedRidBag();
  }

  private ORidBag(byte[] stream) {
    if (stream[0] == 1)
      delegate = new OEmbeddedRidBag();
    else
      delegate = new OSBTreeRidBag();

    delegate.deserialize(stream, 1);
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

  @Override
  public OStringBuilderSerializable toStream(StringBuilder output) throws OSerializationException {
    final ORecordSerializationContext context = ORecordSerializationContext.getContext();
    if (context != null) {
      if (delegate.size() >= topThreshold && isEmbedded()) {
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

    final byte[] stream = new byte[OByteSerializer.BYTE_SIZE + delegate.getSerializedSize()];
    if (isEmbedded())
      stream[0] = 1;
    delegate.serialize(stream, 1);

    output.append(OBase64Utils.encodeBytes(stream));
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
    if (stream[0] == 1)
      delegate = new OEmbeddedRidBag();
    else
      delegate = new OSBTreeRidBag();

    delegate.deserialize(stream, 1);
    return this;
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
}

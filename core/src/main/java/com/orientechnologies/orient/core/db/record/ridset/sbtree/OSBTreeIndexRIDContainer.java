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

package com.orientechnologies.orient.core.db.record.ridset.sbtree;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.orientechnologies.common.profiler.OProfilerMBean;
import com.orientechnologies.common.serialization.types.OBooleanSerializer;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.index.sbtree.OSBTreeMapEntryIterator;
import com.orientechnologies.orient.core.index.sbtree.OTreeInternal;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringBuilderSerializable;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;

/**
 * Persistent Set<OIdentifiable> implementation that uses the SBTree to handle entries in persistent way.
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeIndexRIDContainer implements Set<OIdentifiable>, OStringBuilderSerializable {
  public static final String                    INDEX_FILE_EXTENSION = ".irs";
  private OSBTreeBonsai<OIdentifiable, Boolean> tree;

  protected static final OProfilerMBean         PROFILER             = Orient.instance().getProfiler();

  public OSBTreeIndexRIDContainer(String fileName) {
    tree = new OSBTreeBonsai<OIdentifiable, Boolean>(INDEX_FILE_EXTENSION, false);

    tree.create(fileName, OLinkSerializer.INSTANCE, OBooleanSerializer.INSTANCE,
        (OStorageLocalAbstract) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying());
  }

  public OSBTreeIndexRIDContainer(String fileName, OBonsaiBucketPointer rootPointer) {
    tree = new OSBTreeBonsai<OIdentifiable, Boolean>(INDEX_FILE_EXTENSION, false);
    tree.load(fileName, rootPointer, (OStorageLocalAbstract) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying());
  }

  protected String getFileName() {
    return tree.getName();
  }

  protected OBonsaiBucketPointer getRootPointer() {
    return tree.getRootBucketPointer();
  }

  @Override
  public int size() {
    return (int) tree.size();
  }

  @Override
  public boolean isEmpty() {
    return tree.size() == 0L;
  }

  @Override
  public boolean contains(Object o) {
    return o instanceof OIdentifiable && contains((OIdentifiable) o);
  }

  public boolean contains(OIdentifiable o) {
    return tree.get(o) != null;
  }

  @Override
  public Iterator<OIdentifiable> iterator() {
    return new TreeKeyIterator(tree, false);
  }

  @Override
  public Object[] toArray() {
    // TODO replace with more efficient implementation

    final ArrayList<OIdentifiable> list = new ArrayList<OIdentifiable>(size());

    for (OIdentifiable identifiable : this) {
      list.add(identifiable);
    }

    return list.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    // TODO replace with more efficient implementation.

    final ArrayList<OIdentifiable> list = new ArrayList<OIdentifiable>(size());

    for (OIdentifiable identifiable : this) {
      list.add(identifiable);
    }

    return list.toArray(a);
  }

  @Override
  public boolean add(OIdentifiable oIdentifiable) {
    // TODO check if we can avoid get operation
    if (this.tree.get(oIdentifiable) != null)
      return false;

    this.tree.put(oIdentifiable, Boolean.TRUE);
    return true;
  }

  @Override
  public boolean remove(Object o) {
    return o instanceof OIdentifiable && remove((OIdentifiable) o);
  }

  public boolean remove(OIdentifiable o) {
    return tree.remove(o) != null;
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    for (Object e : c)
      if (!contains(e))
        return false;
    return true;
  }

  @Override
  public boolean addAll(Collection<? extends OIdentifiable> c) {
    boolean modified = false;
    for (OIdentifiable e : c)
      if (add(e))
        modified = true;
    return modified;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    boolean modified = false;
    Iterator<OIdentifiable> it = iterator();
    while (it.hasNext()) {
      if (!c.contains(it.next())) {
        it.remove();
        modified = true;
      }
    }
    return modified;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    boolean modified = false;
    for (Object o : c) {
      modified |= remove(o);
    }

    return modified;
  }

  @Override
  public void clear() {
    tree.clear();
  }

  @Override
  public OSBTreeIndexRIDContainer toStream(StringBuilder iOutput) throws OSerializationException {
    final long timer = PROFILER.startChrono();

    try {
      iOutput.append(OStringSerializerHelper.LINKSET_PREFIX);

      final ODocument document = new ODocument();
      document.field("rootIndex", getRootPointer().getPageIndex());
      document.field("rootOffset", getRootPointer().getPageOffset());
      document.field("file", getFileName());
      iOutput.append(new String(document.toStream()));

      iOutput.append(OStringSerializerHelper.SET_END);
    } finally {
      PROFILER.stopChrono(PROFILER.getProcessMetric("mvrbtree.toStream"), "Serialize a MVRBTreeRID", timer);
    }
    return this;
  }

  public byte[] toStream() {
    final StringBuilder iOutput = new StringBuilder();
    toStream(iOutput);
    return iOutput.toString().getBytes();
  }

  @Override
  public OStringBuilderSerializable fromStream(StringBuilder iInput) throws OSerializationException {
    throw new UnsupportedOperationException("unimplemented yet");
  }

  public static OSBTreeIndexRIDContainer fromStream(String stream) {
    stream = stream.substring(OStringSerializerHelper.LINKSET_PREFIX.length(), stream.length() - 1);

    final ODocument doc = new ODocument();
    doc.fromString(stream);
    final OBonsaiBucketPointer rootIndex = new OBonsaiBucketPointer((Long) doc.field("rootIndex"),
        (Integer) doc.field("rootOffset"));
    final String fileName = doc.field("file");

    return new OSBTreeIndexRIDContainer(fileName, rootIndex);
  }

  private static class TreeKeyIterator implements Iterator<OIdentifiable> {
    private final boolean                                   autoConvertToRecord;
    private OSBTreeMapEntryIterator<OIdentifiable, Boolean> entryIterator;

    public TreeKeyIterator(OTreeInternal<OIdentifiable, Boolean> tree, boolean autoConvertToRecord) {
      entryIterator = new OSBTreeMapEntryIterator<OIdentifiable, Boolean>(tree);
      this.autoConvertToRecord = autoConvertToRecord;
    }

    @Override
    public boolean hasNext() {
      return entryIterator.hasNext();
    }

    @Override
    public OIdentifiable next() {
      final OIdentifiable identifiable = entryIterator.next().getKey();
      if (autoConvertToRecord)
        return identifiable.getRecord();
      else
        return identifiable;
    }

    @Override
    public void remove() {
      entryIterator.remove();
    }
  }

}

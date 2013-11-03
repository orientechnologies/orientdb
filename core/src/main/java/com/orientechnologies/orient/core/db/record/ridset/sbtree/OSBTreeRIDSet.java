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
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.index.sbtree.OSBTreeMapEntryIterator;
import com.orientechnologies.orient.core.index.sbtree.OTreeInternal;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringBuilderSerializable;

/**
 * Persistent Set<OIdentifiable> implementation that uses the SBTree to handle entries in persistent way.
 * 
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeRIDSet implements Set<OIdentifiable>, OStringBuilderSerializable, ORecordLazyMultiValue {
  private final String                   fileName;
  private final OBonsaiBucketPointer     rootPointer;
  private ORecordInternal<?>             owner;
  private boolean                        autoConvertToRecord = true;

  private final OSBTreeCollectionManager collectionManager;

  protected static final OProfilerMBean  PROFILER            = Orient.instance().getProfiler();

  private OSBTreeRIDSet(ODatabaseRecord database) {
    collectionManager = database.getSbTreeCollectionManager();

    OSBTreeBonsai<OIdentifiable, Boolean> tree = collectionManager.createSBTree();
    fileName = tree.getName();
    rootPointer = tree.getRootBucketPointer();
  }

  public OSBTreeRIDSet() {
    this(ODatabaseRecordThreadLocal.INSTANCE.get());
    this.owner = null;
  }

  public OSBTreeRIDSet(ORecordInternal<?> owner) {
    this(owner.getDatabase());
    this.owner = owner;
  }

  public OSBTreeRIDSet(ORecordInternal<?> owner, String fileName, OBonsaiBucketPointer rootPointer) {
    this.owner = owner;
    this.fileName = fileName;
    this.rootPointer = rootPointer;

    if (owner != null)
      collectionManager = owner.getDatabase().getSbTreeCollectionManager();
    else
      collectionManager = ODatabaseRecordThreadLocal.INSTANCE.get().getSbTreeCollectionManager();
  }

  public OSBTreeRIDSet(ODocument owner, Collection<OIdentifiable> iValue) {
    this(owner);
    addAll(iValue);
  }

  private OSBTreeBonsai<OIdentifiable, Boolean> getTree() {
    return collectionManager.loadSBTree(fileName, rootPointer);
  }

  protected String getFileName() {
    return fileName;
  }

  protected OBonsaiBucketPointer getRootPointer() {
    return rootPointer;
  }

  @Override
  public int size() {
    return (int) getTree().size();
  }

  @Override
  public boolean isEmpty() {
    return getTree().size() == 0L;
  }

  @Override
  public boolean contains(Object o) {
    return o instanceof OIdentifiable && contains((OIdentifiable) o);
  }

  public boolean contains(OIdentifiable o) {
    return getTree().get(o) != null;
  }

  @Override
  public Iterator<OIdentifiable> iterator() {
    return new TreeKeyIterator(getTree(), autoConvertToRecord);
  }

  @Override
  public Object[] toArray() {
    // TODO replace with more efficient implementation

    final ArrayList<OIdentifiable> list = new ArrayList<OIdentifiable>(size());

    for (OIdentifiable identifiable : this) {
      if (autoConvertToRecord)
        identifiable = identifiable.getRecord();

      list.add(identifiable);
    }

    return list.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    // TODO replace with more efficient implementation.

    final ArrayList<OIdentifiable> list = new ArrayList<OIdentifiable>(size());

    for (OIdentifiable identifiable : this) {
      if (autoConvertToRecord)
        identifiable = identifiable.getRecord();

      list.add(identifiable);
    }

    return list.toArray(a);
  }

  @Override
  public boolean add(OIdentifiable oIdentifiable) {
    return add(getTree(), oIdentifiable);
  }

  private boolean add(OSBTreeBonsai<OIdentifiable, Boolean> tree, OIdentifiable oIdentifiable) {
    // TODO check if we can avoid get operation
    // TODO fix race condition
    if (getTree().get(oIdentifiable) != null)
      return false;

    getTree().put(oIdentifiable, Boolean.TRUE);
    return true;
  }

  @Override
  public boolean remove(Object o) {
    return o instanceof OIdentifiable && remove((OIdentifiable) o);
  }

  public boolean remove(OIdentifiable o) {
    return getTree().remove(o) != null;
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
    final OSBTreeBonsai<OIdentifiable, Boolean> tree = getTree();
    boolean modified = false;
    for (OIdentifiable e : c)
      if (add(tree, e))
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
    getTree().clear();
  }

  @Override
  public OSBTreeRIDSet toStream(StringBuilder iOutput) throws OSerializationException {
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

  public static OSBTreeRIDSet fromStream(String stream, ORecordInternal<?> owner) {
    stream = stream.substring(OStringSerializerHelper.LINKSET_PREFIX.length(), stream.length() - 1);

    final ODocument doc = new ODocument();
    doc.fromString(stream);
    final OBonsaiBucketPointer rootIndex = new OBonsaiBucketPointer((Long) doc.field("rootIndex"),
        (Integer) doc.field("rootOffset"));
    final String fileName = doc.field("file");

    return new OSBTreeRIDSet(owner, fileName, rootIndex);
  }

  @Override
  public Iterator<OIdentifiable> rawIterator() {
    return new TreeKeyIterator(getTree(), false);
  }

  @Override
  public void convertLinks2Records() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean convertRecords2Links() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isAutoConvertToRecord() {
    return autoConvertToRecord;
  }

  @Override
  public void setAutoConvertToRecord(boolean convertToRecord) {
    autoConvertToRecord = convertToRecord;
  }

  @Override
  public boolean detach() {
    throw new UnsupportedOperationException();
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

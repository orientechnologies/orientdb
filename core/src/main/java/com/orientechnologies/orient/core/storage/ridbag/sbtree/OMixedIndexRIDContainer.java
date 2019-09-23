package com.orientechnologies.orient.core.storage.ridbag.sbtree;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndexEngineException;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class OMixedIndexRIDContainer implements Set<OIdentifiable> {
  private static final String INDEX_FILE_EXTENSION = ".irs";

  private final long                     fileId;
  private final Set<ORID>                embeddedSet;
  private       OIndexRIDContainerSBTree tree         = null;
  private final int                      topThreshold = OGlobalConfiguration.INDEX_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD
      .getValueAsInteger();

  /**
   * Should be called inside of lock to ensure uniqueness of entity on disk !!!
   */
  public OMixedIndexRIDContainer(String name, AtomicLong bonsayFileId) {
    long gotFileId = bonsayFileId.get();
    if (gotFileId == 0) {
      gotFileId = resolveFileIdByName(name + INDEX_FILE_EXTENSION);
      bonsayFileId.set(gotFileId);
    }
    this.fileId = gotFileId;

    embeddedSet = new HashSet<>();
  }

  public OMixedIndexRIDContainer(long fileId, Set<ORID> embeddedSet, OIndexRIDContainerSBTree tree) {
    this.fileId = fileId;
    this.embeddedSet = embeddedSet;
    this.tree = tree;
  }

  private static long resolveFileIdByName(String fileName) {
    final OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) ODatabaseRecordThreadLocal.instance().get().getStorage()
        .getUnderlying();
    boolean rollback = false;
    final OAtomicOperation atomicOperation;
    try {
      atomicOperation = storage.getAtomicOperationsManager().startAtomicOperation(fileName, true);
    } catch (IOException e) {
      throw OException.wrapException(new OIndexEngineException("Error creation of sbtree with name " + fileName, fileName), e);
    }

    try {
      long fileId;

      if (atomicOperation.isFileExists(fileName)) {
        fileId = atomicOperation.loadFile(fileName);
      } else {
        fileId = atomicOperation.addFile(fileName);
      }

      return fileId;
    } catch (IOException e) {
      rollback = true;
      throw OException.wrapException(new OIndexEngineException("Error creation of sbtree with name " + fileName, fileName), e);
    } catch (RuntimeException e) {
      rollback = true;
      throw e;
    } finally {
      try {
        storage.getAtomicOperationsManager().endAtomicOperation(rollback);
      } catch (IOException ioe) {
        OLogManager.instance().error(OMixedIndexRIDContainer.class, "Error of rollback of atomic operation", ioe);
      }
    }
  }

  public long getFileId() {
    return fileId;
  }

  @Override
  public int size() {
    if (tree == null) {
      return embeddedSet.size();
    }

    return embeddedSet.size() + tree.size();
  }

  @Override
  public boolean isEmpty() {
    if (tree == null) {
      return embeddedSet.isEmpty();
    }

    return embeddedSet.isEmpty() && tree.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    if (tree == null) {
      return embeddedSet.contains(o);
    }

    return embeddedSet.contains(o) || tree.contains(o);
  }

  @Override
  public Iterator<OIdentifiable> iterator() {
    if (tree == null) {
      return new Iterator<OIdentifiable>() {
        private final Iterator<ORID> embeddedIterator = embeddedSet.iterator();

        @Override
        public boolean hasNext() {
          return embeddedIterator.hasNext();
        }

        @Override
        public OIdentifiable next() {
          return embeddedIterator.next();
        }
      };
    }

    return new Iterator<OIdentifiable>() {
      private final Iterator<ORID> embeddedIterator = embeddedSet.iterator();
      private final Iterator<OIdentifiable> treeIterator = tree.iterator();

      @Override
      public boolean hasNext() {
        if (embeddedIterator.hasNext()) {
          return true;
        }

        return treeIterator.hasNext();
      }

      @Override
      public OIdentifiable next() {
        if (embeddedIterator.hasNext()) {
          return embeddedIterator.next();
        }

        return treeIterator.next();
      }
    };
  }

  @Override
  public Object[] toArray() {
    if (tree == null) {
      return embeddedSet.toArray();
    }

    final Object[] embeddedArray = embeddedSet.toArray();
    final Object[] treeArray = tree.toArray();

    final Object[] result = new Object[embeddedArray.length + treeArray.length];
    System.arraycopy(embeddedArray, 0, result, 0, embeddedArray.length);
    System.arraycopy(treeArray, 0, result, embeddedArray.length, treeArray.length);

    return result;
  }

  @SuppressWarnings("SuspiciousToArrayCall")
  @Override
  public <T> T[] toArray(T[] a) {
    if (tree == null) {
      return embeddedSet.toArray(a);
    }

    final T[] embeddedArray = embeddedSet.toArray(a);
    final T[] treeArray = tree.toArray(a);

    @SuppressWarnings("unchecked")
    final T[] result = (T[]) java.lang.reflect.Array
        .newInstance(a.getClass().getComponentType(), embeddedArray.length + treeArray.length);

    System.arraycopy(embeddedArray, 0, result, 0, embeddedArray.length);
    System.arraycopy(treeArray, 0, result, embeddedArray.length, treeArray.length);

    return result;
  }

  @Override
  public boolean add(OIdentifiable oIdentifiable) {
    if (embeddedSet.contains(oIdentifiable.getIdentity())) {
      return false;
    }

    if (embeddedSet.size() < topThreshold) {
      return embeddedSet.add(oIdentifiable.getIdentity());
    }

    if (tree == null) {
      final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();
      tree = new OIndexRIDContainerSBTree(fileId, (OAbstractPaginatedStorage) db.getStorage().getUnderlying());
    }

    return tree.add(oIdentifiable);
  }

  public boolean addEntry(OIdentifiable identifiable) {
    if (embeddedSet.contains(identifiable.getIdentity())) {
      return false;
    }

    if (embeddedSet.size() < topThreshold) {
      return embeddedSet.add(identifiable.getIdentity());
    }

    boolean treeWasCreated = false;
    if (tree == null) {
      final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();
      tree = new OIndexRIDContainerSBTree(fileId, (OAbstractPaginatedStorage) db.getStorage().getUnderlying());
      treeWasCreated = true;
    }

    tree.add(identifiable);

    return treeWasCreated;
  }

  @Override
  public boolean remove(Object o) {
    boolean res = embeddedSet.remove(o);
    if (res) {
      return true;
    }

    if (tree == null) {
      return false;
    }

    return tree.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    if (embeddedSet.containsAll(c)) {
      return true;
    }

    if (tree == null) {
      return false;
    }

    final List<?> copy = new ArrayList<>(c);
    //noinspection SuspiciousMethodCalls
    copy.removeAll(embeddedSet);

    return tree.containsAll(copy);
  }

  @Override
  public boolean addAll(Collection<? extends OIdentifiable> c) {
    final int sizeDiff = topThreshold - embeddedSet.size();
    boolean changed = false;

    final Iterator<? extends OIdentifiable> iterator = c.iterator();
    for (int i = 0; i < sizeDiff; i++) {
      if (iterator.hasNext()) {
        final OIdentifiable identifiable = iterator.next();
        changed = changed | embeddedSet.add(identifiable.getIdentity());
      } else {
        return changed;
      }
    }

    if (c.size() > sizeDiff) {
      if (tree == null) {
        final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();
        tree = new OIndexRIDContainerSBTree(fileId, (OAbstractPaginatedStorage) db.getStorage().getUnderlying());
      }

      while (iterator.hasNext()) {
        final OIdentifiable identifiable = iterator.next();
        changed = changed | tree.add(identifiable);
      }
    }

    return changed;
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    boolean changed = embeddedSet.retainAll(c);
    if (tree != null) {
      changed = changed | tree.retainAll(c);
    }

    return changed;
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    boolean changed = embeddedSet.removeAll(c);
    if (tree != null) {
      changed = changed | tree.removeAll(c);
    }

    return changed;
  }

  @Override
  public void clear() {
    embeddedSet.clear();

    if (tree != null) {
      tree.delete();
      tree = null;
    }
  }

  public void delete() {
    if (tree != null) {
      tree.delete();
      tree = null;
    } else if (fileId > 0) {
      final ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().get();
      tree = new OIndexRIDContainerSBTree(fileId, (OAbstractPaginatedStorage) db.getStorage().getUnderlying());
      tree.delete();
    }
  }

  public Set<ORID> getEmbeddedSet() {
    return embeddedSet;
  }

  public OIndexRIDContainerSBTree getTree() {
    return tree;
  }
}

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

package com.orientechnologies.orient.client.remote;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.orientechnologies.common.concur.resource.OCloseable;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.OOrientShutdownListener;
import com.orientechnologies.orient.core.OOrientStartupListener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OBonsaiCollectionPointer;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/** @author Artem Orobets (enisher-at-gmail.com) */
public class OSBTreeCollectionManagerRemote
    implements OCloseable,
        OSBTreeCollectionManager,
        OOrientStartupListener,
        OOrientShutdownListener {

  private static final ConcurrentLinkedHashMap<CacheKey, SBTreeBonsaiContainer> GLOBAL_TREE_CACHE =
      new ConcurrentLinkedHashMap.Builder<CacheKey, SBTreeBonsaiContainer>()
          .maximumWeightedCapacity(Long.MAX_VALUE)
          .build();

  private static final int GLOBAL_EVICTION_THRESHOLD =
      OGlobalConfiguration.SBTREEBONSAI_LINKBAG_CACHE_EVICTION_SIZE.getValueAsInteger();
  private static final int GLOBAL_CACHE_MAX_SIZE =
      OGlobalConfiguration.SBTREEBONSAI_LINKBAG_CACHE_SIZE.getValueAsInteger();

  private static final Object[] GLOBAL_LOCKS;
  private static final int GLOBAL_SHIFT;
  private static final int GLOBAL_MASK;

  static {
    final int concurrencyLevel = Runtime.getRuntime().availableProcessors() * 8;
    int size = 1;

    int shifted = 0;
    while (size < concurrencyLevel) {
      size <<= 1;
      shifted++;
    }

    GLOBAL_SHIFT = 32 - shifted;
    GLOBAL_MASK = size - 1;

    final Object[] locks = new Object[size];
    for (int i = 0; i < locks.length; i++) {
      locks[i] = new Object();
    }

    GLOBAL_LOCKS = locks;
  }

  private final int evictionThreshold;
  private final int cacheMaxSize;
  private final int shift;
  private final int mask;
  private final Object[] locks;
  protected final ConcurrentLinkedHashMap<CacheKey, SBTreeBonsaiContainer> treeCache;
  private final OStorageRemote storage;

  private volatile ThreadLocal<Map<UUID, WeakReference<ORidBag>>> pendingCollections =
      new PendingCollectionsThreadLocal();

  public OSBTreeCollectionManagerRemote(OStorageRemote storage) {
    this(
        GLOBAL_TREE_CACHE, storage, GLOBAL_EVICTION_THRESHOLD, GLOBAL_CACHE_MAX_SIZE, GLOBAL_LOCKS);
  }

  OSBTreeCollectionManagerRemote(OStorageRemote storage, int evictionThreshold, int cacheMaxSize) {
    this(
        new ConcurrentLinkedHashMap.Builder<CacheKey, SBTreeBonsaiContainer>()
            .maximumWeightedCapacity(Long.MAX_VALUE)
            .build(),
        storage,
        evictionThreshold,
        cacheMaxSize,
        null);
  }

  private OSBTreeCollectionManagerRemote(
      ConcurrentLinkedHashMap<CacheKey, SBTreeBonsaiContainer> treeCache,
      OStorageRemote storage,
      int evictionThreshold,
      int cacheMaxSize,
      Object[] locks) {
    this.treeCache = treeCache;
    this.storage = storage;

    this.evictionThreshold = evictionThreshold;
    this.cacheMaxSize = cacheMaxSize;

    if (locks == null) {
      final int concurrencyLevel = Runtime.getRuntime().availableProcessors() * 8;
      int size = 1;

      int shifted = 0;
      while (size < concurrencyLevel) {
        size <<= 1;
        shifted++;
      }

      shift = 32 - shifted;
      mask = size - 1;

      locks = new Object[size];
      for (int i = 0; i < locks.length; i++) {
        locks[i] = new Object();
      }
    } else {
      shift = GLOBAL_SHIFT;
      mask = GLOBAL_MASK;
    }

    this.locks = locks;

    Orient.instance().registerWeakOrientStartupListener(this);
    Orient.instance().registerWeakOrientShutdownListener(this);
  }

  @Override
  public void onShutdown() {
    pendingCollections = null;
    treeCache.clear();
  }

  @Override
  public void onStartup() {
    if (pendingCollections == null) pendingCollections = new PendingCollectionsThreadLocal();
  }

  protected OSBTreeBonsai<OIdentifiable, Integer> createEdgeTree(
      OAtomicOperation atomicOperation, final int clusterId) {
    throw new UnsupportedOperationException(
        "Creation of SB-Tree from remote storage is not allowed");
  }

  protected OSBTreeBonsai<OIdentifiable, Integer> loadTree(
      OBonsaiCollectionPointer collectionPointer) {
    throw new UnsupportedOperationException();
  }

  @Override
  public UUID listenForChanges(ORidBag collection) {
    UUID id = collection.getTemporaryId();
    if (id == null) id = UUID.randomUUID();

    pendingCollections.get().put(id, new WeakReference<ORidBag>(collection));

    return id;
  }

  @Override
  public void updateCollectionPointer(UUID uuid, OBonsaiCollectionPointer pointer) {
    final WeakReference<ORidBag> reference = pendingCollections.get().get(uuid);
    if (reference == null) {
      OLogManager.instance()
          .warn(this, "Update of collection pointer is received but collection is not registered");
      return;
    }

    final ORidBag collection = reference.get();

    if (collection != null) {
      collection.notifySaved(pointer);
    }
  }

  @Override
  public void clearPendingCollections() {
    pendingCollections.get().clear();
  }

  @Override
  public Map<UUID, OBonsaiCollectionPointer> changedIds() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearChangedIds() {
    throw new UnsupportedOperationException();
  }

  private static class PendingCollectionsThreadLocal
      extends ThreadLocal<Map<UUID, WeakReference<ORidBag>>> {
    @Override
    protected Map<UUID, WeakReference<ORidBag>> initialValue() {
      return new HashMap<UUID, WeakReference<ORidBag>>();
    }
  }

  @Override
  public OSBTreeBonsai<OIdentifiable, Integer> createAndLoadTree(
      OAtomicOperation atomicOperation, int clusterId) throws IOException {
    return loadSBTree(createSBTree(clusterId, atomicOperation, null));
  }

  @Override
  public OBonsaiCollectionPointer createSBTree(
      int clusterId, OAtomicOperation atomicOperation, UUID ownerUUID) throws IOException {
    OSBTreeBonsai<OIdentifiable, Integer> tree = createEdgeTree(atomicOperation, clusterId);
    return tree.getCollectionPointer();
  }

  @Override
  public OSBTreeBonsai<OIdentifiable, Integer> loadSBTree(
      OBonsaiCollectionPointer collectionPointer) {
    final CacheKey cacheKey = new CacheKey(storage, collectionPointer);
    final Object lock = treesSubsetLock(cacheKey);

    final OSBTreeBonsai<OIdentifiable, Integer> tree;

    //noinspection SynchronizationOnLocalVariableOrMethodParameter
    synchronized (lock) {
      SBTreeBonsaiContainer container = treeCache.get(cacheKey);
      if (container != null) {
        container.usagesCounter++;
        container.lastAccessTime = System.currentTimeMillis();
        tree = container.tree;
      } else {
        tree = loadTree(collectionPointer);
        if (tree != null) {
          assert tree.getRootBucketPointer().equals(collectionPointer.getRootPointer());

          container = new SBTreeBonsaiContainer(tree);
          container.usagesCounter++;
          container.lastAccessTime = System.currentTimeMillis();

          treeCache.put(cacheKey, container);
        }
      }
    }

    evict();

    return tree;
  }

  @Override
  public void releaseSBTree(OBonsaiCollectionPointer collectionPointer) {
    final CacheKey cacheKey = new CacheKey(storage, collectionPointer);
    final Object lock = treesSubsetLock(cacheKey);
    //noinspection SynchronizationOnLocalVariableOrMethodParameter
    synchronized (lock) {
      SBTreeBonsaiContainer container = treeCache.getQuietly(cacheKey);
      assert container != null;
      container.usagesCounter--;
      assert container.usagesCounter >= 0;
      container.lastAccessTime = System.currentTimeMillis();
    }

    evict();
  }

  @Override
  public void delete(OBonsaiCollectionPointer collectionPointer) {
    final CacheKey cacheKey = new CacheKey(storage, collectionPointer);
    final Object lock = treesSubsetLock(cacheKey);
    //noinspection SynchronizationOnLocalVariableOrMethodParameter
    synchronized (lock) {
      SBTreeBonsaiContainer container = treeCache.getQuietly(cacheKey);
      assert container != null;

      if (container.usagesCounter != 0) {
        throw new IllegalStateException(
            "Cannot delete SBTreeBonsai instance because it is used in other thread.");
      }

      treeCache.remove(cacheKey);
    }
  }

  private void evict() {
    if (treeCache.size() <= cacheMaxSize) {
      return;
    }

    for (CacheKey cacheKey : treeCache.ascendingKeySetWithLimit(evictionThreshold)) {
      final Object treeLock = treesSubsetLock(cacheKey);
      //noinspection SynchronizationOnLocalVariableOrMethodParameter
      synchronized (treeLock) {
        SBTreeBonsaiContainer container = treeCache.getQuietly(cacheKey);
        if (container != null && container.usagesCounter == 0) {
          treeCache.remove(cacheKey);
        }
      }
    }
  }

  @Override
  public void close() {
    clear();
  }

  public void clear() {
    treeCache.keySet().removeIf(cacheKey -> cacheKey.storage == storage);
  }

  void clearClusterCache(final long fileId, String fileName) {
    treeCache
        .entrySet()
        .removeIf(
            entry -> {
              final CacheKey key = entry.getKey();

              if (key.storage == storage && key.pointer.getFileId() == fileId) {
                final SBTreeBonsaiContainer container = entry.getValue();
                if (container.usagesCounter > 0) {
                  throw new IllegalStateException(
                      "Ridbags of file "
                          + fileName
                          + " can not be cleared because some of them are in use");
                }

                return true;
              }

              return false;
            });
  }

  int size() {
    return treeCache.size();
  }

  protected Object treesSubsetLock(CacheKey cacheKey) {
    final int hashCode = cacheKey.hashCode();
    final int index = (hashCode >>> shift) & mask;

    return locks[index];
  }

  protected static final class SBTreeBonsaiContainer {
    private final OSBTreeBonsai<OIdentifiable, Integer> tree;
    protected volatile int usagesCounter = 0;
    protected volatile long lastAccessTime = 0;

    private SBTreeBonsaiContainer(OSBTreeBonsai<OIdentifiable, Integer> tree) {
      this.tree = tree;
    }
  }

  protected static final class CacheKey {
    private final OStorageRemote storage;
    private final OBonsaiCollectionPointer pointer;

    CacheKey(OStorageRemote storage, OBonsaiCollectionPointer pointer) {
      this.storage = storage;
      this.pointer = pointer;
    }

    @Override
    public int hashCode() {
      return storage.hashCode() ^ pointer.hashCode();
    }

    @SuppressWarnings(
        "EqualsWhichDoesntCheckParameterClass") // it's a private class used in a private context
    @Override
    public boolean equals(Object obj) {
      final CacheKey other = (CacheKey) obj;
      return this.storage == other.storage && this.pointer.equals(other.pointer);
    }
  }
}

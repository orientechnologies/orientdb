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

package com.orientechnologies.orient.core.storage.ridbag.sbtree;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.OOrientShutdownListener;
import com.orientechnologies.orient.core.OOrientStartupListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.BTreeBonsaiGlobal;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.btree.BTree;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.btree.EdgeKey;
import com.orientechnologies.orient.core.storage.index.sbtreebonsai.local.OSBTreeBonsai;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/** @author Artem Orobets (enisher-at-gmail.com) */
public final class OSBTreeCollectionManagerShared
    implements OSBTreeCollectionManager, OOrientStartupListener, OOrientShutdownListener {
  public static final String FILE_EXTENSION = ".grb";
  public static final String FILE_NAME_PREFIX = "global_collection_";

  private final OAbstractPaginatedStorage storage;

  private final ConcurrentHashMap<Integer, BTree> fileIdBTreeMap = new ConcurrentHashMap<>();

  private final AtomicLong ridBagIdCounter = new AtomicLong();

  public OSBTreeCollectionManagerShared(OAbstractPaginatedStorage storage) {
    this.storage = storage;
  }

  public void load() {
    final OWriteCache writeCache = storage.getWriteCache();

    for (final Map.Entry<String, Long> entry : writeCache.files().entrySet()) {
      final String fileName = entry.getKey();
      if (fileName.endsWith(FILE_EXTENSION) && fileName.startsWith(FILE_NAME_PREFIX)) {
        final BTree bTree =
            new BTree(
                storage,
                fileName.substring(0, fileName.length() - FILE_EXTENSION.length()),
                FILE_EXTENSION);
        bTree.load();

        fileIdBTreeMap.put(OWOWCache.extractFileId(entry.getValue()), bTree);
        final EdgeKey edgeKey = bTree.lastKey();

        if (edgeKey != null && ridBagIdCounter.get() < edgeKey.ridBagId) {
          ridBagIdCounter.set(edgeKey.ridBagId);
        }
      }
    }
  }

  public void migrate() {}

  @Override
  public OSBTreeBonsai<OIdentifiable, Integer> createAndLoadTree(
      final OAtomicOperation atomicOperation, final int clusterId) {
    return doCreateRidBag(atomicOperation, clusterId);
  }

  public boolean isComponentPresent(final OAtomicOperation operation, final int clusterId) {
    return operation.fileIdByName(generateLockName(clusterId)) >= 0;
  }

  public void createComponent(final OAtomicOperation operation, final int clusterId) {
    // lock is already acquired on storage level, during storage open

    final BTree bTree = new BTree(storage, FILE_NAME_PREFIX + clusterId, FILE_EXTENSION);
    bTree.create(operation);

    final int intFileId = OWOWCache.extractFileId(bTree.getFileId());
    fileIdBTreeMap.put(intFileId, bTree);
  }

  public void deleteComponentByClusterId(
      final OAtomicOperation atomicOperation, final int clusterId) {
    // lock is already acquired on storage level, during cluster drop

    final long fileId = atomicOperation.fileIdByName(generateLockName(clusterId));
    final int intFileId = OWOWCache.extractFileId(fileId);
    final BTree bTree = fileIdBTreeMap.remove(intFileId);

    bTree.delete(atomicOperation);
  }

  private BTreeBonsaiGlobal doCreateRidBag(OAtomicOperation atomicOperation, int clusterId) {
    long fileId = atomicOperation.fileIdByName(generateLockName(clusterId));

    // lock is already acquired on storage level, during start fo the transaction so we
    // are thread safe here.
    if (fileId < 0) {
      final BTree bTree = new BTree(storage, FILE_NAME_PREFIX + clusterId, FILE_EXTENSION);
      bTree.create(atomicOperation);

      fileId = bTree.getFileId();
      final long nextRidBagId = generateNextRidBagId(bTree);

      final int intFileId = OWOWCache.extractFileId(fileId);
      fileIdBTreeMap.put(intFileId, bTree);

      return new BTreeBonsaiGlobal(
          bTree,
          intFileId,
          clusterId,
          nextRidBagId,
          OLinkSerializer.INSTANCE,
          OIntegerSerializer.INSTANCE);
    } else {
      final int intFileId = OWOWCache.extractFileId(fileId);
      final BTree bTree = fileIdBTreeMap.get(intFileId);
      final long nextRidBagId = generateNextRidBagId(bTree);

      return new BTreeBonsaiGlobal(
          bTree,
          intFileId,
          clusterId,
          nextRidBagId,
          OLinkSerializer.INSTANCE,
          OIntegerSerializer.INSTANCE);
    }
  }

  private long generateNextRidBagId(BTree bTree) {
    long nextRidBagId;
    while (true) {
      nextRidBagId = ridBagIdCounter.incrementAndGet();

      if (nextRidBagId < 0) {
        ridBagIdCounter.compareAndSet(nextRidBagId, -nextRidBagId);
        continue;
      }

      try (final Stream<ORawPair<EdgeKey, Integer>> stream =
          bTree.iterateEntriesBetween(
              new EdgeKey(nextRidBagId, Integer.MIN_VALUE, Long.MIN_VALUE),
              true,
              new EdgeKey(nextRidBagId, Integer.MAX_VALUE, Long.MAX_VALUE),
              true,
              true)) {
        if (!stream.findAny().isPresent()) {
          break;
        }
      }
    }
    return nextRidBagId;
  }

  @Override
  public OSBTreeBonsai<OIdentifiable, Integer> loadSBTree(
      OBonsaiCollectionPointer collectionPointer) {
    final int intFileId = (int) collectionPointer.getFileId();

    final BTree bTree = fileIdBTreeMap.get(intFileId);

    return new BTreeBonsaiGlobal(
        bTree,
        intFileId,
        collectionPointer.getRootPointer().getPageOffset(),
        collectionPointer.getRootPointer().getPageIndex(),
        OLinkSerializer.INSTANCE,
        OIntegerSerializer.INSTANCE);
  }

  @Override
  public void releaseSBTree(final OBonsaiCollectionPointer collectionPointer) {}

  @Override
  public void delete(final OBonsaiCollectionPointer collectionPointer) {}

  @Override
  public OBonsaiCollectionPointer createSBTree(
      int clusterId, OAtomicOperation atomicOperation, UUID ownerUUID) {
    final BTreeBonsaiGlobal bonsaiGlobal = doCreateRidBag(atomicOperation, clusterId);
    final OBonsaiCollectionPointer pointer = bonsaiGlobal.getCollectionPointer();

    if (ownerUUID != null) {
      Map<UUID, OBonsaiCollectionPointer> changedPointers =
          ODatabaseRecordThreadLocal.instance().get().getCollectionsChanges();
      if (pointer != null && pointer.isValid()) {
        changedPointers.put(ownerUUID, pointer);
      }
    }

    return pointer;
  }

  /** Change UUID to null to prevent its serialization to disk. */
  @Override
  public UUID listenForChanges(ORidBag collection) {
    UUID ownerUUID = collection.getTemporaryId();
    if (ownerUUID != null) {
      final OBonsaiCollectionPointer pointer = collection.getPointer();
      ODatabaseDocumentInternal session = ODatabaseRecordThreadLocal.instance().get();
      Map<UUID, OBonsaiCollectionPointer> changedPointers = session.getCollectionsChanges();
      if (pointer != null && pointer.isValid()) {
        changedPointers.put(ownerUUID, pointer);
      }
    }

    return null;
  }

  @Override
  public void updateCollectionPointer(UUID uuid, OBonsaiCollectionPointer pointer) {}

  @Override
  public void clearPendingCollections() {}

  @Override
  public Map<UUID, OBonsaiCollectionPointer> changedIds() {
    return ODatabaseRecordThreadLocal.instance().get().getCollectionsChanges();
  }

  @Override
  public void clearChangedIds() {
    ODatabaseRecordThreadLocal.instance().get().getCollectionsChanges().clear();
  }

  @Override
  public void onShutdown() {}

  @Override
  public void onStartup() {}

  public void close() {
    fileIdBTreeMap.clear();
  }

  public boolean delete(
      OAtomicOperation atomicOperation, OBonsaiCollectionPointer collectionPointer) {
    final int fileId = (int) collectionPointer.getFileId();
    final BTree bTree = fileIdBTreeMap.get(fileId);
    if (bTree == null) {
      throw new OStorageException(
          "RidBug for with collection pointer " + collectionPointer + " does not exist");
    }

    final long ridBagId = collectionPointer.getRootPointer().getPageIndex();

    try (Stream<ORawPair<EdgeKey, Integer>> stream =
        bTree.iterateEntriesBetween(
            new EdgeKey(ridBagId, Integer.MIN_VALUE, Long.MIN_VALUE),
            true,
            new EdgeKey(ridBagId, Integer.MAX_VALUE, Long.MAX_VALUE),
            true,
            true)) {
      stream.forEach(pair -> bTree.remove(atomicOperation, pair.first));
    }

    return true;
  }

  /**
   * Generates a lock name for the given cluster ID.
   *
   * @param clusterId the cluster ID to generate the lock name for.
   * @return the generated lock name.
   */
  public static String generateLockName(int clusterId) {
    return FILE_NAME_PREFIX + clusterId + FILE_EXTENSION;
  }
}

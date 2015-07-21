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

package com.orientechnologies.orient.core.db.record.ridbag.sbtree;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.OOrientShutdownListener;
import com.orientechnologies.orient.core.OOrientStartupListener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsaiLocal;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class OSBTreeCollectionManagerShared extends OSBTreeCollectionManagerAbstract implements OOrientStartupListener,
    OOrientShutdownListener {
  private final OAbstractPaginatedStorage                           storage;
  private volatile ThreadLocal<Map<UUID, OBonsaiCollectionPointer>> collectionPointerChanges = new CollectionPointerChangesThreadLocal();

  public OSBTreeCollectionManagerShared() {
    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.get();
    this.storage = (OAbstractPaginatedStorage) db.getStorage().getUnderlying();

    Orient.instance().registerWeakOrientStartupListener(this);
    Orient.instance().registerWeakOrientShutdownListener(this);
  }

  public OSBTreeCollectionManagerShared(int evictionThreshold, int cacheMaxSize, OAbstractPaginatedStorage storage) {
    super(evictionThreshold, cacheMaxSize);
    this.storage = storage;

    Orient.instance().registerWeakOrientStartupListener(this);
    Orient.instance().registerWeakOrientShutdownListener(this);
  }

  @Override
  public void onShutdown() {
    collectionPointerChanges = null;
  }

  @Override
  public void onStartup() {
    if (collectionPointerChanges == null)
      collectionPointerChanges = new CollectionPointerChangesThreadLocal();
  }

  @Override
  public OBonsaiCollectionPointer createSBTree(int clusterId, UUID ownerUUID) {
    final OBonsaiCollectionPointer pointer = super.createSBTree(clusterId, ownerUUID);

    if (ownerUUID != null) {
      Map<UUID, OBonsaiCollectionPointer> changedPointers = collectionPointerChanges.get();
      changedPointers.put(ownerUUID, pointer);
    }

    return pointer;
  }

  @Override
  protected OSBTreeBonsaiLocal<OIdentifiable, Integer> createTree(int clusterId) {

    OSBTreeBonsaiLocal<OIdentifiable, Integer> tree = new OSBTreeBonsaiLocal<OIdentifiable, Integer>(FILE_NAME_PREFIX + clusterId,
        DEFAULT_EXTENSION, true, storage);
    tree.create(OLinkSerializer.INSTANCE, OIntegerSerializer.INSTANCE);

    return tree;
  }

  @Override
  protected OSBTreeBonsai<OIdentifiable, Integer> loadTree(OBonsaiCollectionPointer collectionPointer) {
    String fileName;
    OAtomicOperation atomicOperation = storage.getAtomicOperationsManager().getCurrentOperation();
    if (atomicOperation == null) {
      fileName = storage.getWriteCache().fileNameById(collectionPointer.getFileId());
    } else {
      fileName = atomicOperation.fileNameById(collectionPointer.getFileId());
    }

    OSBTreeBonsaiLocal<OIdentifiable, Integer> tree = new OSBTreeBonsaiLocal<OIdentifiable, Integer>(fileName.substring(0,
        fileName.length() - DEFAULT_EXTENSION.length()), DEFAULT_EXTENSION, true, storage);

    tree.load(collectionPointer.getRootPointer());

    return tree;
  }

  /**
   * Change UUID to null to prevent its serialization to disk.
   * 
   * @param collection
   * @return
   */
  @Override
  public UUID listenForChanges(ORidBag collection) {
    UUID ownerUUID = collection.getTemporaryId();
    if (ownerUUID != null) {
      final OBonsaiCollectionPointer pointer = collection.getPointer();

      Map<UUID, OBonsaiCollectionPointer> changedPointers = collectionPointerChanges.get();
      changedPointers.put(ownerUUID, pointer);
    }

    return null;
  }

  @Override
  public void updateCollectionPointer(UUID uuid, OBonsaiCollectionPointer pointer) {
  }

  @Override
  public void clearPendingCollections() {
  }

  public Map<UUID, OBonsaiCollectionPointer> changedIds() {
    return collectionPointerChanges.get();
  }

  public void clearChangedIds() {
    collectionPointerChanges.get().clear();
  }

  private static class CollectionPointerChangesThreadLocal extends ThreadLocal<Map<UUID, OBonsaiCollectionPointer>> {
    @Override
    protected Map<UUID, OBonsaiCollectionPointer> initialValue() {
      return new HashMap<UUID, OBonsaiCollectionPointer>();
    }
  }
}

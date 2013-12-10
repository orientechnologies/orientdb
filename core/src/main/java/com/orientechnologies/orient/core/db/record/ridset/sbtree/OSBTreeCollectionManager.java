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

import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.common.serialization.types.OBooleanSerializer;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OBonsaiBucketPointer;
import com.orientechnologies.orient.core.index.sbtreebonsai.local.OSBTreeBonsai;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeCollectionManager {

  private final Map<OBonsaiBucketPointer, OSBTreeBonsai<OIdentifiable, Boolean>> treeCache = new HashMap<OBonsaiBucketPointer, OSBTreeBonsai<OIdentifiable, Boolean>>();

  public static final String                                                     FILE_ID   = "ridset";

  public OSBTreeBonsai<OIdentifiable, Boolean> createSBTree() {
    OSBTreeBonsai<OIdentifiable, Boolean> tree = new OSBTreeBonsai<OIdentifiable, Boolean>(".sbt", 1, true);

    tree.create(FILE_ID, OLinkSerializer.INSTANCE, OBooleanSerializer.INSTANCE,
        (OStorageLocalAbstract) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying());

    treeCache.put(tree.getRootBucketPointer(), tree);

    return tree;
  }

  public OSBTreeBonsai<OIdentifiable, Boolean> loadSBTree(long fileId, OBonsaiBucketPointer rootIndex) {
    OSBTreeBonsai<OIdentifiable, Boolean> tree = treeCache.get(rootIndex);
    if (tree != null)
      return tree;

    tree = new OSBTreeBonsai<OIdentifiable, Boolean>(".sbt", 1, true);
    tree.load(fileId, rootIndex, (OStorageLocalAbstract) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getUnderlying());

    treeCache.put(tree.getRootBucketPointer(), tree);

    return tree;
  }

  public void startup() {

  }

  public void shutdown() {
    treeCache.clear();
  }
}

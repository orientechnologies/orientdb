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
import java.util.concurrent.atomic.AtomicLong;

import com.orientechnologies.common.serialization.types.OBooleanSerializer;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.sbtree.local.OSBTree;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OSBTreeCollectionManager {

  private final Map<String, OSBTree<OIdentifiable, Boolean>> treeCache  = new HashMap<String, OSBTree<OIdentifiable, Boolean>>();

  private AtomicLong                                         nextFileId = new AtomicLong();

  public OSBTree<OIdentifiable, Boolean> createSBTree() {
    OSBTree<OIdentifiable, Boolean> tree = new OSBTree<OIdentifiable, Boolean>(".sbt", 1, true);

    final String fileId = "rids" + nextFileId.incrementAndGet();
    tree.create(fileId, OLinkSerializer.INSTANCE, OBooleanSerializer.INSTANCE,
        (OStorageLocalAbstract) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage());

    treeCache.put(fileId, tree);

    return tree;
  }

  public OSBTree<OIdentifiable, Boolean> loadSBTree(String fileName, long rootIndex) {
    OSBTree<OIdentifiable, Boolean> tree = treeCache.get(fileName);
    if (tree != null)
      return tree;

    tree = new OSBTree<OIdentifiable, Boolean>(".sbt", 1, true);
    tree.load(fileName, (OStorageLocalAbstract) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage());

    return tree;
  }

  public void startup() {

  }

  public void shutdown() {
    treeCache.clear();
  }
}

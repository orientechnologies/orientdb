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

  public static final OSBTreeCollectionManager INSTANCE = new OSBTreeCollectionManager();

  public OSBTree<OIdentifiable, Boolean> createSBTree() {
    OSBTree<OIdentifiable, Boolean> tree = new OSBTree<OIdentifiable, Boolean>(".sbt", 1, true);
    tree.create("rids", OLinkSerializer.INSTANCE, OBooleanSerializer.INSTANCE,
        (OStorageLocalAbstract) ODatabaseRecordThreadLocal.INSTANCE.get().getStorage());

    return tree;
  }
}

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
package com.orientechnologies.orient.core.type.tree;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeMapProvider;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeProvider;

/**
 * Persistent MVRB-Tree implementation. The difference with the class OMVRBTreeDatabase is the level. In facts this class works
 * directly at the storage level, while the other at database level. This class is used for Logical Clusters. It can'be
 * transactional.
 * 
 * @see OClusterLogical
 */
@SuppressWarnings("serial")
public class OMVRBTreeStorage<K, V> extends OMVRBTreePersistent<K, V> {

  public OMVRBTreeStorage(OMVRBTreeProvider<K, V> iProvider) {
    super(iProvider);
  }

  public OMVRBTreeStorage(final OAbstractPaginatedStorage iStorage, final String iClusterName, final ORID iRID) {
    super(new OMVRBTreeMapProvider<K, V>(iStorage, iClusterName, iRID));
  }

  public OMVRBTreeStorage(final OAbstractPaginatedStorage iStorage, String iClusterName, final OBinarySerializer<K> iKeySerializer,
      final OStreamSerializer iValueSerializer) {
    super(new OMVRBTreeMapProvider<K, V>(iStorage, iClusterName, iKeySerializer, iValueSerializer));
  }

}

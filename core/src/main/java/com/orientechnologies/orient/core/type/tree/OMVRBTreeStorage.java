/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.type.tree;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.storage.impl.local.OClusterLogical;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.core.type.tree.generic.OTreeDataProviderGeneric;

/**
 * Persistent MVRB-Tree implementation. The difference with the class OMVRBTreeDatabase is the level. In facts this class works
 * directly at the storage level, while the other at database level. This class is used for Logical Clusters. It can'be
 * transactional.
 * 
 * @see OClusterLogical
 */
@SuppressWarnings("serial")
public class OMVRBTreeStorage<K, V> extends OMVRBTreePersistent<K, V> {

	public OMVRBTreeStorage(OTreeDataProvider<K, V> iProvider) {
		super(iProvider);
	}

	public OMVRBTreeStorage(final OStorageLocal iStorage, final String iClusterName, final ORID iRID) {
		super(new OTreeDataProviderGeneric<K, V>(iStorage, iClusterName, iRID));
	}

	public OMVRBTreeStorage(final OStorageLocal iStorage, String iClusterName, final OStreamSerializer iKeySerializer,
			final OStreamSerializer iValueSerializer) {
		super(new OTreeDataProviderGeneric<K, V>(iStorage, iClusterName, iKeySerializer, iValueSerializer));
	}

}

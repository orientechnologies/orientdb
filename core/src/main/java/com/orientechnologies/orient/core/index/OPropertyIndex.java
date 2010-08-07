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
package com.orientechnologies.orient.core.index;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OProperty.INDEX_TYPE;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerListRID;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerString;
import com.orientechnologies.orient.core.type.tree.OTreeMapDatabaseLazySave;

/**
 * Handles indexing when records change.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OPropertyIndex implements Iterable<Entry<String, List<ORecordId>>> {
	protected OProperty																					owner;
	protected OTreeMapDatabaseLazySave<String, List<ORecordId>>	map;

	/**
	 * Constructor called when a new index is created.
	 * 
	 * @param iDatabase
	 *          Current Database instance
	 * @param iProperty
	 *          Owner property
	 * @param iClusterIndexName
	 *          Cluster name where to place the TreeMap
	 */
	public OPropertyIndex(final ODatabaseRecord<?> iDatabase, final OProperty iProperty, final String iClusterIndexName) {
		owner = iProperty;
		map = new OTreeMapDatabaseLazySave<String, List<ORecordId>>(iDatabase, iClusterIndexName, OStreamSerializerString.INSTANCE,
				OStreamSerializerListRID.INSTANCE);
	}

	/**
	 * Constructor called on loading of an existent index.
	 * 
	 * @param iDatabase
	 *          Current Database instance
	 * @param iProperty
	 *          Owner property
	 * @param iRecordId
	 *          Record Id of the persistent TreeMap
	 */
	public OPropertyIndex(final ODatabaseRecord<?> iDatabase, final OProperty iProperty, final ORID iRecordId) {
		owner = iProperty;
		init(iDatabase, iRecordId);
	}

	/**
	 * Constructor called on 2 steps loading of an existent index.
	 * 
	 * @param iDatabase
	 *          Current Database instance
	 * @param iProperty
	 *          Owner property
	 */
	public OPropertyIndex(final ODatabaseRecord<?> iDatabase, final OProperty iProperty) {
		owner = iProperty;
	}

	public abstract INDEX_TYPE getType();

	@SuppressWarnings("unchecked")
	public List<ORecordId> get(Object iKey) {
		final List<ORecordId> values = map.get(iKey);

		if (values == null)
			return Collections.EMPTY_LIST;

		return values;
	}

	public abstract void put(final Object iKey, final ORecordId iValue);

	/**
	 * Populate the index with all the existent records.
	 */
	public void rebuild() {
		Object fieldValue;
		ODocument doc;

		clear();

		final int[] clusterIds = owner.getOwnerClass().getClusterIds();
		for (int clusterId : clusterIds)
			for (Object record : map.getDatabase().browseCluster(map.getDatabase().getClusterNameById(clusterId))) {
				if (record instanceof ODocument) {
					doc = (ODocument) record;
					fieldValue = doc.field(owner.getName());

					if (fieldValue != null)
						put(fieldValue.toString(), (ORecordId) doc.getIdentity());
				}
			}

		lazySave();
	}

	public void remove(final Object key) {
		map.remove(key);
	}

	public void load() throws IOException {
		map.load();
	}

	public void clear() {
		map.clear();
	}

	public void lazySave() {
		map.lazySave();
	}

	public ORecordBytes getRecord() {
		return map.getRecord();
	}

	public Iterator<Entry<String, List<ORecordId>>> iterator() {
		return map.entrySet().iterator();
	}

	protected void init(final ODatabaseRecord<?> iDatabase, final ORID iRecordId) {
		map = new OTreeMapDatabaseLazySave<String, List<ORecordId>>(iDatabase, iRecordId);
		try {
			map.load();
		} catch (IOException e) {
			throw new OIndexException("Can't activate index on property");
		}
	}
}

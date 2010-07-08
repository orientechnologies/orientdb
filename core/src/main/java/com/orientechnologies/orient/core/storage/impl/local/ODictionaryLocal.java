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
package com.orientechnologies.orient.core.storage.impl.local;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.dictionary.ODictionaryInternal;
import com.orientechnologies.orient.core.dictionary.ODictionaryIterator;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyRecord;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerString;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.type.tree.OTreeMapDatabase;

@SuppressWarnings("unchecked")
public class ODictionaryLocal<T extends Object> implements ODictionaryInternal<T> {
	public static final String					DICTIONARY_DEF_CLUSTER_NAME	= OStorage.CLUSTER_INTERNAL_NAME;

	private ODatabaseComplex<T>					database;
	private OTreeMapDatabase<String, T>	tree;

	public String												clusterName									= DICTIONARY_DEF_CLUSTER_NAME;

	public ODictionaryLocal(final ODatabaseRecord<?> iDatabase) throws SecurityException, NoSuchMethodException {
		database = (ODatabaseComplex<T>) iDatabase.getDatabaseOwner();
	}

	public T get(final Object iKey, final String iFetchPlan) {
		return get(iKey, null);
	}

	public T get(final Object iKey) {
		return tree.get(iKey);
	}

	public boolean containsKey(final Object iKey) {
		return tree.containsKey(iKey);
	}

	public ORecordInternal<?> putRecord(final String iKey, final ORecordInternal<?> iValue) {
		return (ORecordInternal<?>) tree.put(iKey, (T) iValue);
	}

	public T put(final String iKey, final T iValue) {
		return tree.put(iKey, iValue);
	}

	public T remove(final Object iKey) {
		return tree.remove(iKey);
	}

	public int size() {
		return tree.size();
	}

	public void load() {
		try {
			tree = new OTreeMapDatabase<String, T>((ODatabaseRecord<?>) database, clusterName, new ORecordId(database.getStorage()
					.getConfiguration().dictionaryRecordId));
			tree.load();
		} catch (IOException e) {
			OLogManager.instance().error(this, "Can't load tree from the database", e, ODatabaseException.class);
		}
	}

	public void create() {
		try {
			tree = new OTreeMapDatabase<String, T>((ODatabaseRecord<?>) database, clusterName, OStreamSerializerString.INSTANCE,
					new OStreamSerializerAnyRecord((ODatabaseRecord<? extends ORecord<?>>) database));
			tree.save();

			database.getStorage().getConfiguration().dictionaryRecordId = tree.getRecord().getIdentity().toString();
			database.getStorage().getConfiguration().update();
		} catch (Exception e) {
			OLogManager.instance().error(this, "Can't create the local database's dictionary", e, ODatabaseException.class);
		}
	}

	public OTreeMapDatabase<String, T> getTree() {
		return tree;
	}

	public Iterator<Entry<String, T>> iterator() {
		return new ODictionaryIterator<T>(tree);
	}

	public Set<String> keySet() {
		return tree.keySet();
	}
}

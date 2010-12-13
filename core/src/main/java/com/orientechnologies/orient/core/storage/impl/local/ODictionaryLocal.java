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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.dictionary.ODictionaryAbstract;
import com.orientechnologies.orient.core.dictionary.ODictionaryIterator;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerAnyRecord;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerString;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeDatabase;

@SuppressWarnings("unchecked")
public class ODictionaryLocal<T extends Object> extends ODictionaryAbstract<T> {
	public static final String						DICTIONARY_DEF_CLUSTER_NAME	= OStorage.CLUSTER_INTERNAL_NAME;

	private ODatabaseRecord<?>						underlyingDatabase;
	private ODatabaseComplex<T>						database;
	private OMVRBTreeDatabase<String, T>	tree;
	private HashSet<String>								transactionalEntries;

	public String													clusterName									= DICTIONARY_DEF_CLUSTER_NAME;

	public ODictionaryLocal(final ODatabaseRecord<?> iDatabase) throws SecurityException, NoSuchMethodException {
		super(iDatabase);
		underlyingDatabase = iDatabase;
		database = (ODatabaseComplex<T>) iDatabase.getDatabaseOwner();
	}

	public T get(final Object iKey, final String iFetchPlan) {
		return tree.get(iKey, iFetchPlan);
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
		T prev = tree.put(iKey, iValue);

		if (iValue instanceof ORecord<?> && ((ORecord<?>) iValue).getIdentity().isTemporary()) {
			if (transactionalEntries == null)
				transactionalEntries = new HashSet<String>();

			// REMEMBER THE KEY TO RE-SET WHEN THE TX IS COMMITTED AND RID ARE NOT MORE TEMPORARIES
			transactionalEntries.add(iKey);
		}

		return prev;
	}

	public T remove(final Object iKey) {
		return tree.remove(iKey);
	}

	public int size() {
		return tree.size();
	}

	public void load() {
		tree = new OMVRBTreeDatabase<String, T>(underlyingDatabase, new ORecordId(
				database.getStorage().getConfiguration().dictionaryRecordId));
		tree.load();
	}

	public void create() {
		try {
			tree = new OMVRBTreeDatabase<String, T>(underlyingDatabase, clusterName, OStreamSerializerString.INSTANCE,
					OStreamSerializerAnyRecord.INSTANCE);
			tree.save();

			database.getStorage().getConfiguration().dictionaryRecordId = tree.getRecord().getIdentity().toString();
			database.getStorage().getConfiguration().update();
		} catch (Exception e) {
			OLogManager.instance().error(this, "Can't create the local database's dictionary", e, ODatabaseException.class);
		}
	}

	public OMVRBTreeDatabase<String, T> getTree() {
		return tree;
	}

	public Iterator<Entry<String, T>> iterator() {
		return new ODictionaryIterator<T>(tree);
	}

	public Set<String> keySet() {
		return tree.keySet();
	}
}

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
package com.orientechnologies.orient.core.tx;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges.OPERATION;
import com.orientechnologies.orient.core.tx.OTransactionIndexChangesPerKey.OTransactionIndexEntry;

public abstract class OTransactionRealAbstract extends OTransactionAbstract {
	protected Map<ORID, OTransactionRecordEntry>		allEntries				= new HashMap<ORID, OTransactionRecordEntry>();
	protected Map<ORID, OTransactionRecordEntry>		recordEntries			= new HashMap<ORID, OTransactionRecordEntry>();
	protected Map<String, OTransactionIndexChanges>	indexEntries			= new HashMap<String, OTransactionIndexChanges>();
	protected int																		id;
	protected int																		newObjectCounter	= -2;

	protected OTransactionRealAbstract(ODatabaseRecordTx iDatabase, int iId) {
		super(iDatabase);
		id = iId;
	}

	public void close() {
		allEntries.clear();
		recordEntries.clear();
		indexEntries.clear();
		newObjectCounter = -2;
		status = TXSTATUS.INVALID;
		
		database.setDefaultTransactionMode();
	}

	public int getId() {
		return id;
	}

	public void clearRecordEntries() {
		allEntries.putAll(recordEntries);
		recordEntries.clear();
	}

	public Collection<OTransactionRecordEntry> getCurrentRecordEntries() {
		return recordEntries.values();
	}

	public Collection<OTransactionRecordEntry> getAllRecordEntries() {
		return allEntries.values();
	}

	public OTransactionRecordEntry getRecordEntry(final ORecordId rid) {
		OTransactionRecordEntry e = recordEntries.get(rid);
		if (e != null)
			return e;

		e = allEntries.get(rid);
		if (e != null)
			return e;

		return null;
	}

	public ORecordInternal<?> getRecord(final ORecordId rid) {
		final OTransactionRecordEntry e = getRecordEntry(rid);
		if (e != null && e.status != OTransactionRecordEntry.DELETED)
			return e.record;
		return null;
	}

	/**
	 * Called by class iterator.
	 */
	public List<OTransactionRecordEntry> getRecordEntriesByClass(final String iClassName) {
		final List<OTransactionRecordEntry> result = new ArrayList<OTransactionRecordEntry>();

		if (iClassName == null || iClassName.length() == 0)
			// RETURN ALL THE RECORDS
			for (OTransactionRecordEntry entry : recordEntries.values()) {
				result.add(entry);
			}
		else
			// FILTER RECORDS BY CLASSNAME
			for (OTransactionRecordEntry entry : recordEntries.values()) {
				if (entry.getRecord() != null && entry.getRecord() instanceof ODocument
						&& iClassName.equals(((ODocument) entry.getRecord()).getClassName()))
					result.add(entry);
			}

		return result;
	}

	/**
	 * Called by cluster iterator.
	 */
	public List<OTransactionRecordEntry> getRecordEntriesByClusterIds(final int[] iIds) {
		final List<OTransactionRecordEntry> result = new ArrayList<OTransactionRecordEntry>();

		if (iIds == null)
			// RETURN ALL THE RECORDS
			for (OTransactionRecordEntry entry : recordEntries.values()) {
				result.add(entry);
			}
		else
			// FILTER RECORDS BY ID
			for (OTransactionRecordEntry entry : recordEntries.values()) {
				for (int id : iIds) {
					if (entry.getRecord() != null && entry.getRecord().getIdentity().getClusterId() == id) {
						result.add(entry);
						break;
					}
				}
			}

		return result;
	}

	public void clearIndexEntries() {
		indexEntries.clear();
	}

	public List<String> getInvolvedIndexes() {
		List<String> list = null;
		for (String indexName : indexEntries.keySet()) {
			if (list == null)
				list = new ArrayList<String>();
			list.add(indexName);
		}
		return list;
	}

	public ODocument getIndexChanges() {
		final StringBuilder value = new StringBuilder();

		final ODocument result = new ODocument();

		for (Entry<String, OTransactionIndexChanges> indexEntry : indexEntries.entrySet()) {
			final ODocument indexDoc = new ODocument();
			result.field(indexEntry.getKey(), indexDoc);

			if (indexEntry.getValue().cleared)
				indexDoc.field("clear", Boolean.TRUE);

			final List<ODocument> entries = new ArrayList<ODocument>();
			indexDoc.field("entries", entries);

			// STORE INDEX ENTRIES
			for (OTransactionIndexChangesPerKey entry : indexEntry.getValue().changesPerKey.values()) {
				// SERIALIZE KEY
				value.setLength(0);
				if (entry.key != null)
					ORecordSerializerStringAbstract.fieldTypeToString(value, null, OType.getTypeByClass(entry.key.getClass()), entry.key);
				else
					value.append('*');
				String key = value.toString();

				final List<ODocument> operations = new ArrayList<ODocument>();

				// SERIALIZE VALUES
				if (entry.entries != null && !entry.entries.isEmpty()) {
					for (OTransactionIndexEntry e : entry.entries) {
						final ODocument changeDoc = new ODocument();

						// SERIALIZE OPERATION
						changeDoc.field("o", e.operation.ordinal());

						if (e.value instanceof ORecord<?> && e.value.getIdentity().isNew())
							((ORecord<?>) e.value).save();

						changeDoc.field("v", e.value.getIdentity());

						operations.add(changeDoc);
					}
				}

				entries.add(new ODocument().field("k", OStringSerializerHelper.encode(key)).field("ops", operations));
			}
		}

		indexEntries.clear();

		return result;
	}

	/**
	 * Bufferizes index changes to be flushed at commit time.
	 * 
	 * @return
	 */
	public OTransactionIndexChanges getIndexChanges(final String iIndexName) {
		return indexEntries.get(iIndexName);
	}

	/**
	 * Bufferizes index changes to be flushed at commit time.
	 */
	public void addIndexEntry(final OIndex delegate, final String iIndexName, final OTransactionIndexChanges.OPERATION iOperation,
			final Object iKey, final OIdentifiable iValue) {
		OTransactionIndexChanges indexEntry = indexEntries.get(iIndexName);
		if (indexEntry == null) {
			indexEntry = new OTransactionIndexChanges();
			indexEntries.put(iIndexName, indexEntry);
		}

		if (iOperation == OPERATION.CLEAR)
			indexEntry.setCleared();
		else
			indexEntry.getChangesPerKey(iKey).add(iValue, iOperation);
	}

	protected void checkTransaction() {
		if (status == TXSTATUS.INVALID)
			throw new OTransactionException("Invalid state of the transaction. The transaction must be begun.");
	}
}

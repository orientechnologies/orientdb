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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Basic interface to handle index.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OIndex<T> {

	/**
	 * Creates the index.
	 * 
	 * @param iName
	 * 
	 * @param iDatabase
	 *          Current Database instance
	 * @param iProperty
	 *          Owner property
	 * @param iClusterIndexName
	 *          Cluster name where to place the TreeMap
	 * @param iClusterIdsToIndex
	 * @param iProgressListener
	 *          Listener to get called on progress
	 * @param iAutomatic
	 */
	public OIndex<T> create(String iName, final OType iKeyType, final ODatabaseRecord iDatabase, final String iClusterIndexName,
			final int[] iClusterIdsToIndex, final OProgressListener iProgressListener, final boolean iAutomatic);

	public void unload();

	public OType getKeyType();

	public Iterator<Entry<Object, T>> iterator();

	/**
	 * Gets the set of records associated with the passed key.
	 * 
	 * @param iKey
	 *          Key to search
	 * @return The Record set if found, otherwise an empty Set
	 */
	public T get(Object iKey);

	public boolean contains(Object iKey);

	public OIndex<T> put(final Object iKey, final OIdentifiable iValue);

	public boolean remove(final Object iKey);

	public boolean remove(Object iKey, OIdentifiable iRID);

	/**
	 * Removes a value in all the index entries.
	 * 
	 * @param iRecord
	 *          Record to search
	 * @return Times the record was found, 0 if not found at all
	 */
	public int remove(OIdentifiable iRID);

	public OIndex<T> clear();

	public Iterable<Object> keys();

	/**
	 * Returns a set of records with keys greater than passed parameter.
	 * 
	 * @param fromKey
	 *          Starting key.
	 * @param isInclusive
	 *          Indicates whether record with passed key will be included.
	 * 
	 * @return set of records with keys greater than passed parameter.
	 */
	public abstract Collection<OIdentifiable> getValuesMajor(Object fromKey, boolean isInclusive);

	/**
	 * Returns a set of records with keys less than passed parameter.
	 * 
	 * @param toKey
	 *          Ending key.
	 * @param isInclusive
	 *          Indicates whether record with passed key will be included.
	 * 
	 * @return set of records with keys less than passed parameter.
	 */
	public abstract Collection<OIdentifiable> getValuesMinor(Object toKey, boolean isInclusive);

	/**
	 * Returns a set of documents that contains fields ("key", "rid") where "key" - index key, "rid" - record id of records with keys
	 * greater than passed parameter.
	 * 
	 * @param fromKey
	 *          Starting key.
	 * @param isInclusive
	 *          Indicates whether record with passed key will be included.
	 * 
	 * @return set of records with key greater than passed parameter.
	 */
	public abstract Collection<ODocument> getEntriesMajor(Object fromKey, boolean isInclusive);

	/**
	 * Returns a set of documents that contains fields ("key", "rid") where "key" - index key, "rid" - record id of records with keys
	 * less than passed parameter.
	 * 
	 * @param toKey
	 *          Ending key.
	 * @param isInclusive
	 *          Indicates whether record with passed key will be included.
	 * 
	 * @return set of records with key greater than passed parameter.
	 */
	public abstract Collection<ODocument> getEntriesMinor(Object toKey, boolean isInclusive);

	/**
	 * Returns a set of records with key between the range passed as parameter.
	 * 
	 * @param iRangeFrom
	 *          Starting range
	 * @param iRangeTo
	 *          Ending range
	 * @param iInclusive
	 *          Include from/to bounds
	 * @see #getValuesBetween(Object, Object)
	 * @return
	 */
	public abstract Collection<OIdentifiable> getValuesBetween(final Object iRangeFrom, final Object iRangeTo,
			final boolean iInclusive);

	/**
	 * Returns a set of documents with key between the range passed as parameter.
	 * 
	 * @param iRangeFrom
	 *          Starting range
	 * @param iRangeTo
	 *          Ending range
	 * @param iInclusive
	 *          Include from/to bounds
	 * @see #getEntriesBetween(Object, Object)
	 * @return
	 */
	public abstract Collection<ODocument> getEntriesBetween(final Object iRangeFrom, final Object iRangeTo, final boolean iInclusive);

	public Collection<OIdentifiable> getValuesBetween(Object iRangeFrom, Object iRangeTo);

	public Collection<ODocument> getEntriesBetween(Object iRangeFrom, Object iRangeTo);

	public long getSize();

	public OIndex<T> lazySave();

	public OIndex<T> delete();

	public String getName();

	public String getType();

	public boolean isAutomatic();

	public long rebuild();

	/**
	 * Populate the index with all the existent records.
	 */
	public long rebuild(final OProgressListener iProgressListener);

	public void setCallback(final OIndexCallback iCallback);

	public ODocument getConfiguration();

	public ORID getIdentity();

	/**
	 * Commit changes as atomic.
	 * 
	 * @param oDocument
	 *          Collection of entries to commit
	 */
	public void commit(ODocument iDocument);

	public OIndexInternal<T> getInternal();
}

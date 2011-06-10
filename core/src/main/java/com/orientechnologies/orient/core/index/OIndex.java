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
import java.util.Set;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Basic interface to handle index.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OIndex {

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
	public OIndex create(String iName, final ODatabaseRecord iDatabase, final String iClusterIndexName,
			final int[] iClusterIdsToIndex, final OProgressListener iProgressListener, final boolean iAutomatic);

	public void unload();

	public Iterator<Entry<Object, Set<OIdentifiable>>> iterator();

	/**
	 * Gets the set of records associated with the passed key.
	 * 
	 * @param iKey
	 *          Key to search
	 * @return The Record set if found, otherwise an empty Set
	 */
	public Collection<OIdentifiable> get(Object iKey);

	public boolean contains(Object iKey);

	public OIndex put(final Object iKey, final OIdentifiable iValue);

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

	public OIndex clear();

	public Iterable<Object> keys();

	public Collection<OIdentifiable> getBetween(Object iRangeFrom, Object iRangeTo);

	public long getSize();

	public OIndex lazySave();

	public OIndex delete();

	public String getName();

	public String getType();

	public boolean isAutomatic();

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
}

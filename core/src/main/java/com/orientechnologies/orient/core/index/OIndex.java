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

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Interface to handle index.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OIndex extends Iterable<Entry<Object, Set<ORecord<?>>>> {

	public static final String	CONFIG_TYPE				= "type";
	public static final String	CONFIG_NAME				= "name";
	public static final String	CONFIG_AUTOMATIC	= "automatic";

	public String getName();

	public Set<ORecord<?>> get(Object iKey);

	public OIndex put(final Object iKey, final ORecord<?> iValue);

	public OIndex remove(final Object iKey);

	public OIndex remove(Object iKey, ORecord<?> iRID);

	public OIndex clear();

	public OIndex rebuild();

	/**
	 * Populate the index with all the existent records.
	 */
	public OIndex rebuild(final OProgressListener iProgressListener);

	public int getSize();

	public String getType();

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

	public OIndex load();

	public OIndex delete();

	public OIndex lazySave();

	public Iterator<Entry<Object, Set<ORecord<?>>>> iterator();

	public ORID getIdentity();

	public void checkEntry(final ODocument iRecord, final Object iKey);

	public void setCallback(final OIndexCallback iCallback);

	public OIndex setName(String iName);

	public void unload();

	public ODocument getConfiguration();

	public OIndex loadFromConfiguration(ODocument iConfig);

	public ODocument updateConfiguration();

	public boolean isAutomatic();

	public Set<ORecord<?>> getBetween(Object iRangeFrom, Object iRangeTo);

}

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
import java.util.List;
import java.util.Map.Entry;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OProperty.INDEX_TYPE;

/**
 * Interface to handle indexes at property level.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public interface OPropertyIndex extends Iterable<Entry<String, List<ORecordId>>> {
	public INDEX_TYPE getType();

	public Object getIdentity();

	public void put(final Object iKey, final ORecordId iValue);

	/**
	 * Creates the index.
	 * 
	 * @param iDatabase
	 *          Current Database instance
	 * @param iProperty
	 *          Owner property
	 * @param iClusterIndexName
	 *          Cluster name where to place the TreeMap
	 * @param iProgressListener
	 *          Listener to get called on progress
	 */
	public OPropertyIndex create(final ODatabaseRecord iDatabase, final OProperty iProperty, final String iClusterIndexName,
			final OProgressListener iProgressListener);

	public OPropertyIndex configure(final ODatabaseRecord iDatabase, final OProperty iProperty, final ORID iRecordId);

	public List<ORecordId> get(Object iKey);

	public void rebuild();

	/**
	 * Populate the index with all the existent records.
	 */
	public void rebuild(final OProgressListener iProgressListener);

	public void remove(final Object iKey);

	public void load();

	public void clear();

	public void delete();

	public void lazySave();

	public Iterator<Entry<String, List<ORecordId>>> iterator();

	public int getIndexedItems();
}

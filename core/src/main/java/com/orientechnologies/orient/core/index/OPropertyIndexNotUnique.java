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

import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OProperty.INDEX_TYPE;

/**
 * Handles indexing when records change.
 * 
 * @author Luca Garulli
 * 
 */
public class OPropertyIndexNotUnique extends OPropertyIndex {

	public OPropertyIndexNotUnique(final ODatabaseRecord<?> iDatabase, final OProperty iProperty, final String iClusterIndexName) {
		super(iDatabase, iProperty, iClusterIndexName);
	}

	/**
	 * Constructor called on loading of an existent index.
	 * 
	 * @param iDatabase
	 *          Current Database instance
	 * @param iProperty
	 *          Owner property
	 * @param iClusterIndexName
	 *          Cluster name where to place the TreeMap
	 * @param iRecordId
	 *          Record Id of the persistent TreeMap
	 */
	public OPropertyIndexNotUnique(final ODatabaseRecord<?> iDatabase, final OProperty iProperty, final ORID iRecordId) {
		super(iDatabase, iProperty, iRecordId);
	}

	public void put(final Object iKey, final ORecordId iSingleValue) {
		List<ORecordId> values = map.get(iKey);
		if (values == null)
			values = new ArrayList<ORecordId>();

		int pos = values.indexOf(iSingleValue);
		if (pos > -1)
			// REPLACE IT
			values.set(pos, iSingleValue);
		else
			values.add(iSingleValue);

		map.put(iKey.toString(), values);
	}

	@Override
	public INDEX_TYPE getType() {
		return INDEX_TYPE.NOT_UNIQUE;
	}

	@Override
	public ORID getRID() {
		return map.getRecord().getIdentity();
	}
}

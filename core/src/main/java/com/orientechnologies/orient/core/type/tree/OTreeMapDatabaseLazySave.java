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

import com.orientechnologies.orient.core.config.OConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;

/**
 * Collects changes all together and save them following the selected strategy. By default the map is saved automatically every
 * "maxUpdatesBeforeSave" updates (=500). "maxUpdatesBeforeSave" is configurable: 0 means no automatic save, 1 means non-lazy map
 * (save each operation) and > 1 is lazy.
 * 
 * @author Luca Garulli
 */
@SuppressWarnings("serial")
public class OTreeMapDatabaseLazySave<K, V> extends OTreeMapDatabase<K, V> implements ODatabaseLifecycleListener {
	protected int	maxUpdatesBeforeSave;
	protected int	updates	= 0;

	public OTreeMapDatabaseLazySave(ODatabaseRecord<?> iDatabase, ORID iRID) {
		super(iDatabase, iRID);
		init(iDatabase);
	}

	public OTreeMapDatabaseLazySave(ODatabaseRecord<?> iDatabase, String iClusterName, OStreamSerializer iKeySerializer,
			OStreamSerializer iValueSerializer) {
		super(iDatabase, iClusterName, iKeySerializer, iValueSerializer);
		init(iDatabase);
	}

	/**
	 * Do nothing since all the changes will be committed expressly at lazySave() time or on closing.
	 */
	@Override
	public synchronized void commitChanges(final ODatabaseRecord<?> iDatabase) {
		if (maxUpdatesBeforeSave > 0 && ++updates >= maxUpdatesBeforeSave) {
			lazySave();
			updates = 0;
		}
	}

	@Override
	public void clear() {
		super.clear();
		lazySave();
	}

	public void lazySave() {
		super.commitChanges(database);
		optimize();
	}

	public void onOpen(final ODatabase iDatabase) {
	}

	public void onTxRollback(ODatabase iDatabase) {
	}

	public void onTxCommit(ODatabase iDatabase) {
		super.commitChanges(database);
	}

	/**
	 * Assure to save all the data without the optimization.
	 */
	public void onClose(final ODatabase iDatabase) {
		super.commitChanges(database);
		cache.clear();
		entryPoints.clear();
		entryPointsSize = 0;
		root = null;
	}

	/**
	 * Returns the maximum updates to save the map persistently.
	 * 
	 * @return 0 means no automatic save, 1 means non-lazy map (save each operation) and > 1 is lazy.
	 */
	public int getMaxUpdatesBeforeSave() {
		return maxUpdatesBeforeSave;
	}

	/**
	 * Sets the maximum updates to save the map persistently.
	 * 
	 * @param iValue
	 *          0 means no automatic save, 1 means non-lazy map (save each operation) and > 1 is lazy.
	 */
	public void setMaxUpdatesBeforeSave(final int iValue) {
		this.maxUpdatesBeforeSave = iValue;
	}

	private void init(ODatabaseRecord<?> iDatabase) {
		iDatabase.registerListener(this);
		maxUpdatesBeforeSave = OConfiguration.TREEMAP_LAZY_UPDATES.getValueAsInteger();
	}
}

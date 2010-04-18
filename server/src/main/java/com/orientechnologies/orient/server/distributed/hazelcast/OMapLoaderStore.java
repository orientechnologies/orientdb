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
package com.orientechnologies.orient.server.distributed.hazelcast;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Instance;
import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStore;
import com.hazelcast.core.Instance.InstanceType;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.enterprise.distributed.ODistributedException;
import com.orientechnologies.orient.enterprise.distributed.hazelcast.ODistributedRecordId;
import com.orientechnologies.orient.server.OServerMain;

@SuppressWarnings("unchecked")
public class OMapLoaderStore implements MapLoader<ODistributedRecordId, ORawBuffer>, MapStore<ODistributedRecordId, ORawBuffer> {
	public OMapLoaderStore() {
		OLogManager.instance().config(this, "OMapLoaderStore started");
	}

	/**
	 * Load the record using the local storage.
	 */
	public ORawBuffer load(final ODistributedRecordId iRecordId) {
		OLogManager.instance().config(this, "Loading record from database: %s", iRecordId);

		try {
			OStorage storage = getLocalStorage(iRecordId.dbName);

			return storage.readRecord(0, iRecordId.rid);

		} catch (Exception e) {
			throw new ODistributedException("Error on loading record: " + iRecordId, e);
		}
	}

	/**
	 * Store the record in the local storage.
	 */
	public void store(final ODistributedRecordId iRecordId, final ORawBuffer iValue) {
		try {
			OStorage storage = getLocalStorage(iRecordId.dbName);

			if (iRecordId.rid.isValid()) {
				OLogManager.instance().config(this, "Updating record to database: %s", iRecordId);
				storage.updateRecord(0, iRecordId.rid, iValue.buffer, iValue.version, iValue.recordType);
			} else {
				OLogManager.instance().config(this, "Creating record to database: %s", iRecordId);
				iRecordId.rid.clusterPosition = storage.createRecord(0, iValue.buffer, iValue.recordType);

				// RE-SEND THE RECORD AS UPDATE TO THE SAME MAP
				for (Instance instance : Hazelcast.getInstances()) {
					if (instance.getInstanceType() == InstanceType.MAP && instance.getId().toString().substring(2).equals(iRecordId.dbName)) {
						((IMap<ODistributedRecordId, ORawBuffer>) instance).put(iRecordId, iValue);
						break;
					}
				}
			}

		} catch (Exception e) {
			throw new ODistributedException("Error on saving record: " + iRecordId, e);
		}
	}

	/**
	 * Delete a record from the local storage.
	 */
	public void delete(final ODistributedRecordId iRecordId) {
		if (!iRecordId.rid.isValid())
			return;

		OLogManager.instance().config(this, "Deleting record from database: %s", iRecordId);

		try {
			OStorage storage = getLocalStorage(iRecordId.dbName);

			storage.deleteRecord(0, iRecordId.rid, iRecordId.version);

		} catch (Exception e) {
			throw new ODistributedException("Error on deleting record: " + iRecordId, e);
		}
	}

	/**
	 * Load, in one shot, all the records with the ids specified.
	 */
	public Map<ODistributedRecordId, ORawBuffer> loadAll(final Collection<ODistributedRecordId> iRecordIds) {
		Map<ODistributedRecordId, ORawBuffer> entries = new HashMap<ODistributedRecordId, ORawBuffer>();

		for (ODistributedRecordId k : iRecordIds) {
			entries.put(k, load(k));
		}

		return entries;
	}

	/**
	 * Store, in one shot, all the records with the ids specified.
	 */
	public void storeAll(final Map<ODistributedRecordId, ORawBuffer> iEntries) {
		for (Map.Entry<ODistributedRecordId, ORawBuffer> entry : iEntries.entrySet())
			store(entry.getKey(), entry.getValue());
	}

	/**
	 * Delete, in one shot, all the records with the ids specified.
	 */
	public void deleteAll(final Collection<ODistributedRecordId> iRecordIds) {
		for (ODistributedRecordId k : iRecordIds)
			delete(k);
	}

	public static OStorage getLocalStorage(final String iDbName) throws IOException {
		OStorage stg = Orient.instance().accessToLocalStorage(OServerMain.server().getStoragePath(iDbName), "rw");
		if (stg.isClosed())
			stg.open(0, null, null);
		return stg;
	}
}

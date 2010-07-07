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
package com.orientechnologies.orient.core.db.raw;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OCacheRecord;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OClusterLocal;
import com.orientechnologies.orient.core.storage.impl.local.OClusterLogical;

@SuppressWarnings("unchecked")
public class ODatabaseRaw implements ODatabase {
	private static volatile int	serialId		= 0;

	protected int								id;
	protected OStorage					storage;
	protected STATUS						status;

	private ODatabaseRecord<?>	databaseOwner;

	private boolean							useCache		= true;
	private Map<String, Object>	properties	= new HashMap<String, Object>();

	public enum STATUS {
		OPEN, CLOSED
	}

	public ODatabaseRaw(final String iURL) {
		try {
			storage = Orient.instance().getStorage(iURL);
			id = serialId++;
			status = STATUS.CLOSED;

			// SET DEFAULT PROPERTIES
			setProperty("fetch-max", 50);

		} catch (Throwable t) {
			throw new ODatabaseException("Error on opening database '" + iURL + "'", t);
		}
	}

	public <DB extends ODatabase> DB open(final String iUserName, final String iUserPassword) {
		try {
			if (status == STATUS.OPEN)
				throw new IllegalStateException("Database " + getName() + " is already open");

			storage.open(getId(), iUserName, iUserPassword);

			status = STATUS.OPEN;
		} catch (ODatabaseException e) {
			throw e;
		} catch (Exception e) {
			throw new ODatabaseException("Can't open database", e);
		}
		return (DB) this;
	}

	public <DB extends ODatabase> DB create() {
		try {
			storage.create();
			status = STATUS.OPEN;
		} catch (Exception e) {
			throw new ODatabaseException("Can't create database", e);
		}
		return (DB) this;
	}

	public boolean exists() {
		if (status == STATUS.OPEN)
			return true;

		return storage.exists();
	}

	public long countClusterElements(final String iClusterName) {
		return storage.count(getClusterIdByName(iClusterName));
	}

	public long countClusterElements(final int iClusterId) {
		return storage.count(iClusterId);
	}

	public long countClusterElements(final int[] iClusterIds) {
		return storage.count(iClusterIds);
	}

	public ORawBuffer read(final int iClusterId, final long iPosition) {
		try {

			final String recId = ORecordId.generateString(iClusterId, iPosition);

			// SEARCH IT IN CACHE
			ORawBuffer result;

			if (useCache) {
				// FIND IN CACHE
				result = getCache().findRecord(recId);

				if (result != null)
					// FOUND: JUST RETURN IT
					return result;
			}

			result = storage.readRecord(id, iClusterId, iPosition);

			if (useCache)
				// ADD THE RECORD TO THE LOCAL CACHE
				getCache().addRecord(recId, result);

			return result;

		} catch (Throwable t) {
			throw new ODatabaseException("Error on retrieving record #" + iPosition + " in cluster '"
					+ storage.getPhysicalClusterNameById(iClusterId) + "'", t);
		}
	}

	public long save(final int iClusterId, long iPosition, final byte[] iContent, final int iVersion, final byte iRecordType) {
		try {
			if (iPosition == ORID.CLUSTER_POS_INVALID) {
				// CREATE
				iPosition = storage.createRecord(iClusterId, iContent, iRecordType);

				if (useCache)
					// ADD/UPDATE IT IN CACHE
					getCache().addRecord(ORecordId.generateString(iClusterId, iPosition), new ORawBuffer(iContent, 0, iRecordType));

				return iPosition;
			} else {
				// UPDATE
				int newVersion = storage.updateRecord(id, iClusterId, iPosition, iContent, iVersion, iRecordType);

				if (useCache)
					// ADD/UPDATE IT IN CACHE
					getCache().addRecord(ORecordId.generateString(iClusterId, iPosition), new ORawBuffer(iContent, newVersion, iRecordType));

				return newVersion;
			}
		} catch (Throwable t) {
			throw new ODatabaseException("Error on saving record in cluster id: " + iClusterId + ", position: " + iPosition, t);
		}
	}

	public void delete(final String iClusterName, final long iPosition, final int iVersion) {
		delete(getClusterIdByName(iClusterName), iPosition, iVersion);
	}

	public void delete(final int iClusterId, final long iPosition, final int iVersion) {
		try {
			if (!storage.deleteRecord(id, iClusterId, iPosition, iVersion))
				throw new ORecordNotFoundException("The record with id '" + iClusterId + ":" + iPosition + "' was not found");

		} catch (Exception e) {
			OLogManager.instance().exception("Error on deleting record #%d in cluster '%s'", e, ODatabaseException.class, iPosition,
					storage.getPhysicalClusterNameById(iClusterId));
		}
	}

	public OStorage getStorage() {
		return storage;
	}

	public boolean isClosed() {
		return status == STATUS.CLOSED;
	}

	public String getName() {
		return storage != null ? storage.getName() : "<no-name>";
	}

	@Override
	public void finalize() {
		close();
	}

	public void close() {
		if (status != STATUS.OPEN)
			return;

		if (storage != null)
			storage.removeUser();

		status = STATUS.CLOSED;
	}

	public int getId() {
		return id;
	}

	public String getClusterType(final String iClusterName) {
		return storage.getClusterById(storage.getClusterIdByName(iClusterName)).getType();
	}

	public int getClusterIdByName(final String iClusterName) {
		return storage.getClusterIdByName(iClusterName);
	}

	public String getClusterNameById(final int iClusterId) {
		if (iClusterId == -1)
			return null;

		// PHIYSICAL CLUSTER
		return storage.getPhysicalClusterNameById(iClusterId);
	}

	public int addLogicalCluster(final String iClusterName, final int iPhyClusterContainerId) {
		return storage.addCluster(iClusterName, OClusterLogical.TYPE, iPhyClusterContainerId);
	}

	public int addPhysicalCluster(final String iClusterName, final String iClusterFileName, final int iStartSize) {
		return storage.addCluster(iClusterName, OClusterLocal.TYPE, iClusterFileName, iStartSize);
	}

	public int addDataSegment(final String iSegmentName, final String iSegmentFileName) {
		return storage.addDataSegment(iSegmentName, iSegmentFileName);
	}

	public Collection<String> getClusterNames() {
		return storage.getClusterNames();
	}

	public OCacheRecord getCache() {
		return storage.getCache();
	}

	public int getDefaultClusterId() {
		return storage.getDefaultClusterId();
	}

	public void declareIntent(final OIntent iIntent, final Object... iParams) {
		iIntent.activate(this, iParams);
	}

	public ODatabaseRecord<?> getDatabaseOwner() {
		return databaseOwner;
	}

	public ODatabaseRaw setOwner(final ODatabaseRecord<?> iOwner) {
		databaseOwner = iOwner;
		return this;
	}

	public boolean isUseCache() {
		return useCache;
	}

	public void setUseCache(boolean useCache) {
		this.useCache = useCache;
	}

	public Object setProperty(final String iName, final Object iValue) {
		return properties.put(iName, iValue);
	}

	public Object getProperty(final String iName) {
		return properties.get(iName);
	}

	public Iterator<Entry<String, Object>> getProperties() {
		return properties.entrySet().iterator();
	}
}

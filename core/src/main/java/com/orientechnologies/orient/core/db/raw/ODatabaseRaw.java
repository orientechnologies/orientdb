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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OLevel1RecordCache;
import com.orientechnologies.orient.core.cache.OLevel2RecordCache;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.ODatabaseListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;

/**
 * Lower level ODatabase implementation. It's extended or wrapped by all the others.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("unchecked")
public class ODatabaseRaw implements ODatabase {
	protected String											url;
	protected OStorage										storage;
	protected STATUS											status;
	protected OIntent											currentIntent;

	private ODatabaseRecord								databaseOwner;
	private final Map<String, Object>			properties	= new HashMap<String, Object>();
	private final List<ODatabaseListener>	listeners		= new ArrayList<ODatabaseListener>();

	public ODatabaseRaw(final String iURL) {
		try {
			url = iURL.replace('\\', '/');
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

			if (storage == null)
				storage = Orient.instance().loadStorage(url);
			storage.open(iUserName, iUserPassword, properties);

			// WAKE UP DB LIFECYCLE LISTENER
			for (ODatabaseLifecycleListener it : Orient.instance().getDbLifecycleListeners())
				it.onOpen(getDatabaseOwner());

			// WAKE UP LISTENERS
			for (ODatabaseListener listener : listeners)
				try {
					listener.onOpen(this);
				} catch (Throwable t) {
				}

			status = STATUS.OPEN;
		} catch (OException e) {
			// PASS THROUGH
			throw e;
		} catch (Exception e) {
			throw new ODatabaseException("Can't open database", e);
		}
		return (DB) this;
	}

	public <DB extends ODatabase> DB create() {
		try {
			if (status == STATUS.OPEN)
				throw new IllegalStateException("Database " + getName() + " is already open");

			if (storage == null)
				storage = Orient.instance().loadStorage(url);
			storage.create(properties);

			// WAKE UP DB LIFECYCLE LISTENER
			for (ODatabaseLifecycleListener it : Orient.instance().getDbLifecycleListeners())
				it.onOpen(getDatabaseOwner());

			// WAKE UP LISTENERS
			for (ODatabaseListener listener : listeners)
				try {
					listener.onCreate(this);
				} catch (Throwable t) {
				}

			status = STATUS.OPEN;
		} catch (Exception e) {
			throw new ODatabaseException("Can't create database", e);
		}
		return (DB) this;
	}

	public void delete() {
		final List<ODatabaseListener> tmpListeners = new ArrayList<ODatabaseListener>(listeners);
		close();

		try {
			if (storage == null)
				storage = Orient.instance().loadStorage(url);

			storage.delete();
			storage = null;

			// WAKE UP LISTENERS
			for (ODatabaseListener listener : tmpListeners)
				try {
					listener.onDelete(this);
				} catch (Throwable t) {
				}

			status = STATUS.CLOSED;
		} catch (OException e) {
			// PASS THROUGH
			throw e;
		} catch (Exception e) {
			throw new ODatabaseException("Can't delete database", e);
		}
	}

	public void reload() {
		storage.reload();
	}

	public STATUS getStatus() {
		return status;
	}

	public <DB extends ODatabase> DB setStatus(final STATUS status) {
		this.status = status;
		return (DB) this;
	}

	public boolean exists() {
		if (status == STATUS.OPEN)
			return true;

		if (storage == null)
			storage = Orient.instance().loadStorage(url);

		return storage.exists();
	}

	public long countClusterElements(final String iClusterName) {
		final int clusterId = getClusterIdByName(iClusterName);
		if (clusterId < 0)
			throw new IllegalArgumentException("Cluster '" + iClusterName + "' was not found");
		return storage.count(clusterId);
	}

	public long countClusterElements(final int iClusterId) {
		return storage.count(iClusterId);
	}

	public long countClusterElements(final int[] iClusterIds) {
		return storage.count(iClusterIds);
	}

	public ORawBuffer read(final ORecordId iRid, final String iFetchPlan) {
		if (!iRid.isValid())
			return null;

		OFetchHelper.checkFetchPlanValid(iFetchPlan);

		try {
			return storage.readRecord(databaseOwner, iRid, iFetchPlan);

		} catch (Throwable t) {
			if (iRid.isTemporary())
				throw new ODatabaseException("Error on retrieving record using temporary RecordId: " + iRid, t);
			else
				throw new ODatabaseException("Error on retrieving record " + iRid + " (cluster: "
						+ storage.getPhysicalClusterNameById(iRid.clusterId) + ")", t);
		}
	}

	public long save(final ORecordId iRid, final byte[] iContent, final int iVersion, final byte iRecordType) {
		// CHECK IF RECORD TYPE IS SUPPORTED
		Orient.instance().getRecordFactoryManager().getRecordTypeClass(iRecordType);

		try {
			if (iRid.clusterPosition < 0) {
				// CREATE
				return storage.createRecord(iRid, iContent, iRecordType);

			} else {
				// UPDATE
				return storage.updateRecord(iRid, iContent, iVersion, iRecordType);
			}
		} catch (OException e) {
			// PASS THROUGH
			throw e;
		} catch (Throwable t) {
			throw new ODatabaseException("Error on saving record " + iRid, t);
		}
	}

	public void delete(final ORecordId iRid, final int iVersion) {
		try {
			if (!storage.deleteRecord(iRid, iVersion))
				throw new ORecordNotFoundException("The record with id " + iRid + " was not found");

		} catch (OException e) {
			// PASS THROUGH
			throw e;
		} catch (Exception e) {
			OLogManager.instance().exception("Error on deleting record " + iRid, e, ODatabaseException.class);
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

	public String getURL() {
		return url != null ? url : storage.getURL();
	}

	@Override
	public void finalize() {
		close();
	}

	public int getClusters() {
		return storage.getClusters();
	}

	public String getClusterType(final String iClusterName) {
		return storage.getClusterTypeByName(iClusterName);
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

	public long getClusterRecordSizeById(final int iClusterId) {
		try {
			return storage.getClusterById(iClusterId).getRecordsSize();
		} catch (Exception e) {
			OLogManager.instance().exception("Error on reading records size for cluster with id '" + iClusterId + "'", e,
					ODatabaseException.class);
		}
		return 0l;
	}

	public long getClusterRecordSizeByName(final String iClusterName) {
		try {
			return storage.getClusterById(getClusterIdByName(iClusterName)).getRecordsSize();
		} catch (Exception e) {
			OLogManager.instance().exception("Error on reading records size for cluster '" + iClusterName + "'", e,
					ODatabaseException.class);
		}
		return 0l;
	}

	public int addCluster(final String iClusterName, final OStorage.CLUSTER_TYPE iType) {
		return storage.addCluster(iClusterName, iType);
	}

	public int addLogicalCluster(final String iClusterName, final int iPhyClusterContainerId) {
		return storage.addCluster(iClusterName, OStorage.CLUSTER_TYPE.LOGICAL, iPhyClusterContainerId);
	}

	public int addPhysicalCluster(final String iClusterName, final String iClusterFileName, final int iStartSize) {
		return storage.addCluster(iClusterName, OStorage.CLUSTER_TYPE.PHYSICAL, iClusterFileName, iStartSize);
	}

	public boolean dropCluster(final String iClusterName) {
		return storage.dropCluster(iClusterName);
	}

	public boolean dropCluster(int iClusterId) {
		return storage.dropCluster(iClusterId);
	}

	public int addDataSegment(final String iSegmentName, final String iSegmentFileName) {
		return storage.addDataSegment(iSegmentName, iSegmentFileName);
	}

	public Collection<String> getClusterNames() {
		return storage.getClusterNames();
	}

	/**
	 * Returns always null
	 * 
	 * @return
	 */
	public OLevel1RecordCache getLevel1Cache() {
		return null;
	}

	public int getDefaultClusterId() {
		return storage.getDefaultClusterId();
	}

	public boolean declareIntent(final OIntent iIntent) {
		if (currentIntent != null) {
			if (iIntent != null && iIntent.getClass().equals(currentIntent.getClass()))
				// SAME INTENT: JUMP IT
				return false;

			// END CURRENT INTENT
			currentIntent.end(this);
		}

		currentIntent = iIntent;

		if (iIntent != null)
			iIntent.begin(this);

		return true;
	}

	public ODatabaseRecord getDatabaseOwner() {
		return databaseOwner;
	}

	public ODatabaseRaw setOwner(final ODatabaseRecord iOwner) {
		databaseOwner = iOwner;
		return this;
	}

	public Object setProperty(final String iName, final Object iValue) {
		if (iValue == null)
			return properties.remove(iName.toLowerCase());
		else
			return properties.put(iName.toLowerCase(), iValue);
	}

	public Object getProperty(final String iName) {
		return properties.get(iName.toLowerCase());
	}

	public Iterator<Entry<String, Object>> getProperties() {
		return properties.entrySet().iterator();
	}

	public void registerListener(final ODatabaseListener iListener) {
		listeners.add(iListener);
	}

	public void unregisterListener(final ODatabaseListener iListener) {
		for (int i = 0; i < listeners.size(); ++i)
			if (listeners.get(i) == iListener) {
				listeners.remove(i);
				break;
			}
	}

	public List<ODatabaseListener> getListeners() {
		return listeners;
	}

	public OLevel2RecordCache getLevel2Cache() {
		return storage.getLevel2Cache();
	}

	public void close() {
		if (status != STATUS.OPEN)
			return;

		callOnCloseListeners();
		listeners.clear();

		if (storage != null)
			storage.close();

		storage = null;
		status = STATUS.CLOSED;
	}

	@Override
	public String toString() {
		return "OrientDB[" + (getStorage() != null ? getStorage().getURL() : "?") + "]";
	}

	public Object get(final ATTRIBUTES iAttribute) {
		if (iAttribute == null)
			throw new IllegalArgumentException("attribute is null");

		switch (iAttribute) {
		case STATUS:
			return getStatus();
		}

		return null;
	}

	public <DB extends ODatabase> DB set(final ATTRIBUTES iAttribute, final Object iValue) {
		if (iAttribute == null)
			throw new IllegalArgumentException("attribute is null");

		final String stringValue = iValue != null ? iValue.toString() : null;

		switch (iAttribute) {
		case STATUS:
			setStatus(STATUS.valueOf(stringValue.toUpperCase(Locale.ENGLISH)));
			break;
		}

		return (DB) this;
	}

	public <V> V callInLock(Callable<V> iCallable, boolean iExclusiveLock) {
		return storage.callInLock(iCallable, iExclusiveLock);
	}

	public void callOnCloseListeners() {
		// WAKE UP DB LIFECYCLE LISTENER
		for (ODatabaseLifecycleListener it : Orient.instance().getDbLifecycleListeners())
			it.onClose(getDatabaseOwner());

		// WAKE UP LISTENERS
		for (ODatabaseListener listener : listeners)
			try {
				listener.onClose(getDatabaseOwner());
			} catch (Throwable t) {
				t.printStackTrace();
			}
	}

	protected boolean isClusterBoundedToClass(int iClusterId) {
		return false;
	}
}

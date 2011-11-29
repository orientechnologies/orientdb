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
package com.orientechnologies.orient.core.db;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import com.orientechnologies.orient.core.cache.OLevel1RecordCache;
import com.orientechnologies.orient.core.cache.OLevel2RecordCache;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.OStorage.CLUSTER_TYPE;

@SuppressWarnings("unchecked")
public abstract class ODatabaseWrapperAbstract<DB extends ODatabase> implements ODatabase {
	protected DB									underlying;
	protected ODatabaseComplex<?>	databaseOwner;

	public ODatabaseWrapperAbstract(final DB iDatabase) {
		underlying = iDatabase;
		databaseOwner = (ODatabaseComplex<?>) this;
	}

	@Override
	public void finalize() {
		// close();
	}

	public <THISDB extends ODatabase> THISDB open(final String iUserName, final String iUserPassword) {
		underlying.open(iUserName, iUserPassword);
		return (THISDB) this;
	}

	public <THISDB extends ODatabase> THISDB create() {
		underlying.create();
		return (THISDB) this;
	}

	public boolean exists() {
		return underlying.exists();
	}

	public void reload() {
		underlying.reload();
	}

	public void close() {
		underlying.close();
	}

	public void delete() {
		underlying.delete();
	}

	public STATUS getStatus() {
		return underlying.getStatus();
	}

	public <THISDB extends ODatabase> THISDB setStatus(final STATUS iStatus) {
		underlying.setStatus(iStatus);
		return (THISDB) this;
	}

	public String getName() {
		return underlying.getName();
	}

	public String getURL() {
		return underlying.getURL();
	}

	public OStorage getStorage() {
		return underlying.getStorage();
	}

	public OLevel1RecordCache getLevel1Cache() {
		checkOpeness();
		return underlying.getLevel1Cache();
	}

	public OLevel2RecordCache getLevel2Cache() {
		checkOpeness();
		return getStorage().getLevel2Cache();
	}

	public boolean isClosed() {
		return underlying.isClosed();
	}

	public long countClusterElements(final int iClusterId) {
		checkOpeness();
		return underlying.countClusterElements(iClusterId);
	}

	public long countClusterElements(final int[] iClusterIds) {
		checkOpeness();
		return underlying.countClusterElements(iClusterIds);
	}

	public long countClusterElements(final String iClusterName) {
		checkOpeness();
		return underlying.countClusterElements(iClusterName);
	}

	public int getClusters() {
		checkOpeness();
		return underlying.getClusters();
	}

	public Collection<String> getClusterNames() {
		checkOpeness();
		return underlying.getClusterNames();
	}

	public String getClusterType(final String iClusterName) {
		checkOpeness();
		return underlying.getClusterType(iClusterName);
	}

	public int getClusterIdByName(final String iClusterName) {
		checkOpeness();
		return underlying.getClusterIdByName(iClusterName);
	}

	public String getClusterNameById(final int iClusterId) {
		checkOpeness();
		return underlying.getClusterNameById(iClusterId);
	}

	public long getClusterRecordSizeById(int iClusterId) {
		return underlying.getClusterRecordSizeById(iClusterId);
	}

	public long getClusterRecordSizeByName(String iClusterName) {
		return underlying.getClusterRecordSizeByName(iClusterName);
	}

	public int addCluster(final String iClusterName, final CLUSTER_TYPE iType) {
		checkOpeness();
		return underlying.addCluster(iClusterName, iType);
	}

	@Deprecated
	public int addLogicalCluster(final String iClusterName, final int iPhyClusterContainerId) {
		checkOpeness();
		return underlying.addLogicalCluster(iClusterName, iPhyClusterContainerId);
	}

	public int addPhysicalCluster(final String iClusterName, final String iClusterFileName, final int iStartSize) {
		checkOpeness();
		return underlying.addPhysicalCluster(iClusterName, iClusterFileName, iStartSize);
	}

	public int addPhysicalCluster(final String iClusterName) {
		checkOpeness();
		return underlying.addPhysicalCluster(iClusterName, iClusterName, -1);
	}

	public boolean dropCluster(final String iClusterName) {
		getLevel1Cache().freeCluster(getClusterIdByName(iClusterName));
		return underlying.dropCluster(iClusterName);
	}

	public boolean dropCluster(final int iClusterId) {
		getLevel1Cache().freeCluster(iClusterId);
		return underlying.dropCluster(iClusterId);
	}

	public int addDataSegment(final String iSegmentName, final String iSegmentFileName) {
		checkOpeness();
		return underlying.addDataSegment(iSegmentName, iSegmentFileName);
	}

	public int getDefaultClusterId() {
		checkOpeness();
		return underlying.getDefaultClusterId();
	}

	public boolean declareIntent(final OIntent iIntent) {
		checkOpeness();
		return underlying.declareIntent(iIntent);
	}

	public <DBTYPE extends ODatabase> DBTYPE getUnderlying() {
		return (DBTYPE) underlying;
	}

	public ODatabaseComplex<?> getDatabaseOwner() {
		return databaseOwner;
	}

	public ODatabaseComplex<?> setDatabaseOwner(final ODatabaseComplex<?> iOwner) {
		databaseOwner = iOwner;
		return (ODatabaseComplex<?>) this;
	}

	@Override
	public boolean equals(final Object iOther) {
		if (!(iOther instanceof ODatabase))
			return false;

		final ODatabase other = (ODatabase) iOther;

		return other.getName().equals(getName());
	}

	@Override
	public String toString() {
		return underlying.toString();
	}

	public Object setProperty(final String iName, final Object iValue) {
		return underlying.setProperty(iName, iValue);
	}

	public Object getProperty(final String iName) {
		return underlying.getProperty(iName);
	}

	public Iterator<Entry<String, Object>> getProperties() {
		return underlying.getProperties();
	}

	public Object get(final ATTRIBUTES iAttribute) {
		return underlying.get(iAttribute);
	}

	public <THISDB extends ODatabase> THISDB set(final ATTRIBUTES attribute, final Object iValue) {
		return (THISDB) underlying.set(attribute, iValue);
	}

	public void registerListener(final ODatabaseListener iListener) {
		underlying.registerListener(iListener);
	}

	public void unregisterListener(final ODatabaseListener iListener) {
		underlying.unregisterListener(iListener);
	}

	public <V> V callInLock(Callable<V> iCallable, boolean iExclusiveLock) {
		return getStorage().callInLock(iCallable, iExclusiveLock);
	}

	protected void checkOpeness() {
		if (isClosed())
			throw new ODatabaseException("Database '" + getURL() + "' is closed");
	}
}

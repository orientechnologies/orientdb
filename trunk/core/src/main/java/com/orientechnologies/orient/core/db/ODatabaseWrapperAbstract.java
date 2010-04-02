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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.cache.OCacheRecord;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.storage.OStorage;

@SuppressWarnings("unchecked")
public abstract class ODatabaseWrapperAbstract<DB extends ODatabase, REC extends ORecordInternal<?>> implements ODatabase {
	protected DB									underlying;
	protected OUser								user;
	protected ODatabaseComplex<?>	databaseOwner;

	public ODatabaseWrapperAbstract(final DB iDatabase) {
		underlying = iDatabase;
		databaseOwner = (ODatabaseComplex<?>) this;
	}

	@Override
	public void finalize() {
		close();
	}

	public <THISDB extends ODatabase> THISDB open(final String iUserName, final String iUserPassword) {
		underlying.open(iUserName, iUserPassword);
		return (THISDB) this;
	}

	public <THISDB extends ODatabase> THISDB create(final String iStorageMode) {
		underlying.create(iStorageMode);
		return (THISDB) this;
	}

	public boolean exists() {
		return underlying.exists();
	}

	public void close() {
		underlying.close();
	}

	public int getId() {
		return underlying.getId();
	}

	public String getName() {
		return underlying.getName();
	}

	public OStorage getStorage() {
		return underlying.getStorage();
	}

	public OCacheRecord getCache() {
		checkOpeness();
		return underlying.getCache();
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

	public Collection<String> getClusterNames() {
		checkOpeness();
		return underlying.getClusterNames();
	}

	public int getClusterIdByName(final String iClusterName) {
		checkOpeness();
		return underlying.getClusterIdByName(iClusterName);
	}

	public String getClusterNameById(final int iClusterId) {
		checkOpeness();
		return underlying.getClusterNameById(iClusterId);
	}

	public int addLogicalCluster(final String iClassName, final int iPhyClusterContainerId) {
		checkOpeness();
		return underlying.addLogicalCluster(iClassName, iPhyClusterContainerId);
	}

	public int addPhysicalCluster(final String iClusterName, final String iClusterFileName, final int iStartSize) {
		checkOpeness();
		return underlying.addPhysicalCluster(iClusterName, iClusterFileName, iStartSize);
	}

	public int addDataSegment(final String iSegmentName, final String iSegmentFileName) {
		checkOpeness();
		return underlying.addDataSegment(iSegmentName, iSegmentFileName);
	}

	public int getDefaultClusterId() {
		checkOpeness();
		return underlying.getDefaultClusterId();
	}

	public void declareIntent(final OIntent iIntent, final Object... iParams) {
		checkOpeness();
		underlying.declareIntent(iIntent, iParams);
	}

	public <DBTYPE extends ODatabase> DBTYPE checkSecurity(final String iResource, final int iOperation) {
		return (DBTYPE) underlying.checkSecurity(iResource, iOperation);
	}

	public <DBTYPE extends ODatabase> DBTYPE getUnderlying() {
		return (DBTYPE) underlying;
	}

	protected void checkOpeness() {
		if (isClosed())
			OLogManager.instance().error(this, "Database is closed", ODatabaseException.class);
	}

	public ODatabaseComplex<?> getDatabaseOwner() {
		return databaseOwner;
	}

	public ODatabaseComplex<?> setDatabaseOwner(final ODatabaseComplex<?> iOwner) {
		databaseOwner = iOwner;
		return (ODatabaseComplex<?>) this;
	}
}

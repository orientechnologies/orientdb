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

import java.util.List;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.hook.ORecordHook.TYPE;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.storage.OStorage.CLUSTER_TYPE;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;

@SuppressWarnings("unchecked")
public abstract class ODatabaseRecordWrapperAbstract<DB extends ODatabaseRecord> extends ODatabaseWrapperAbstract<DB> implements
		ODatabaseComplex<ORecordInternal<?>> {

	public ODatabaseRecordWrapperAbstract(final DB iDatabase) {
		super(iDatabase);
		iDatabase.setDatabaseOwner(this);
	}

	@Override
	public <THISDB extends ODatabase> THISDB create() {
		checkSecurity(ODatabaseSecurityResources.DATABASE, ORole.PERMISSION_CREATE);
		return (THISDB) super.create();
	}

	@Override
	public void delete() {
		checkSecurity(ODatabaseSecurityResources.DATABASE, ORole.PERMISSION_DELETE);
		super.delete();
	}

	public int addCluster(final String iClusterName, final CLUSTER_TYPE iType) {
		checkSecurity(ODatabaseSecurityResources.DATABASE, ORole.PERMISSION_UPDATE);
		return super.addCluster(iClusterName, iType);
	}

	@Override
	public int addLogicalCluster(final String iClassName, final int iPhyClusterContainerId) {
		checkSecurity(ODatabaseSecurityResources.DATABASE, ORole.PERMISSION_UPDATE);
		return super.addLogicalCluster(iClassName, iPhyClusterContainerId);
	}

	@Override
	public int addPhysicalCluster(final String iClusterName, final String iClusterFileName, final int iStartSize) {
		checkSecurity(ODatabaseSecurityResources.DATABASE, ORole.PERMISSION_UPDATE);
		return super.addPhysicalCluster(iClusterName, iClusterFileName, iStartSize);
	}

	@Override
	public int addPhysicalCluster(final String iClusterName) {
		checkSecurity(ODatabaseSecurityResources.DATABASE, ORole.PERMISSION_UPDATE);
		return super.addPhysicalCluster(iClusterName);
	}

	@Override
	public boolean dropCluster(final String iClusterName) {
		checkSecurity(ODatabaseSecurityResources.DATABASE, ORole.PERMISSION_UPDATE);
		checkClusterBoundedToClass(getClusterIdByName(iClusterName));
		return super.dropCluster(iClusterName);
	}

	public boolean dropCluster(int iClusterId) {
		checkSecurity(ODatabaseSecurityResources.DATABASE, ORole.PERMISSION_UPDATE);
		checkClusterBoundedToClass(iClusterId);
		return super.dropCluster(iClusterId);
	}

	@Override
	public int addDataSegment(final String iSegmentName, final String iSegmentFileName) {
		checkSecurity(ODatabaseSecurityResources.DATABASE, ORole.PERMISSION_UPDATE);
		return super.addDataSegment(iSegmentName, iSegmentFileName);
	}

	public OTransaction getTransaction() {
		return underlying.getTransaction();
	}

	public ODatabaseComplex<ORecordInternal<?>> begin() {
		return underlying.begin();
	}

	public ODatabaseComplex<ORecordInternal<?>> begin(final TXTYPE iType) {
		return underlying.begin(iType);
	}

	public ODatabaseComplex<ORecordInternal<?>> begin(final OTransaction iTx) {
		return underlying.begin(iTx);
	}

	public boolean isMVCC() {
		checkOpeness();
		return underlying.isMVCC();
	}

	public <RET extends ODatabaseComplex<?>> RET setMVCC(final boolean iValue) {
		checkOpeness();
		return (RET) underlying.setMVCC(iValue);
	}

	public boolean isValidationEnabled() {
		return underlying.isValidationEnabled();
	}

	public <RET extends ODatabaseRecord> RET setValidationEnabled(final boolean iValue) {
		return (RET) underlying.setValidationEnabled(iValue);
	}

	public OUser getUser() {
		return underlying.getUser();
	}

	public OMetadata getMetadata() {
		return underlying.getMetadata();
	}

	public ODictionary<ORecordInternal<?>> getDictionary() {
		return underlying.getDictionary();
	}

	public byte getRecordType() {
		return underlying.getRecordType();
	}

	public <REC extends ORecordInternal<?>> ORecordIteratorCluster<REC> browseCluster(final String iClusterName) {
		return underlying.browseCluster(iClusterName);
	}

	public <REC extends ORecordInternal<?>> ORecordIteratorCluster<REC> browseCluster(final String iClusterName,
			final Class<REC> iRecordClass) {
		return underlying.browseCluster(iClusterName, iRecordClass);
	}

	public <RET extends OCommandRequest> RET command(final OCommandRequest iCommand) {
		return (RET) underlying.command(iCommand);
	}

	public <RET extends List<?>> RET query(final OQuery<? extends Object> iCommand, final Object... iArgs) {
		return (RET) underlying.query(iCommand, iArgs);
	}

	public <RET extends Object> RET newInstance() {
		return (RET) underlying.newInstance();
	}

	public ODatabaseComplex<ORecordInternal<?>> delete(final ORecordInternal<?> iRecord) {
		underlying.delete(iRecord);
		return this;
	}

	public <RET extends ORecordInternal<?>> RET load(final ORID iRecordId) {
		return (RET) underlying.load(iRecordId);
	}

	public <RET extends ORecordInternal<?>> RET load(final ORID iRecordId, final String iFetchPlan) {
		return (RET) underlying.load(iRecordId, iFetchPlan);
	}

	public <RET extends ORecordInternal<?>> RET load(final ORID iRecordId, final String iFetchPlan, final boolean iIgnoreCache) {
		return (RET) underlying.load(iRecordId, iFetchPlan, iIgnoreCache);
	}

	public <RET extends ORecordInternal<?>> RET getRecord(final OIdentifiable iIdentifiable) {
		return (RET) underlying.getRecord(iIdentifiable);
	}

	public <RET extends ORecordInternal<?>> RET load(final ORecordInternal<?> iRecord) {
		return (RET) underlying.load(iRecord);
	}

	public <RET extends ORecordInternal<?>> RET load(final ORecordInternal<?> iRecord, final String iFetchPlan) {
		return (RET) underlying.load(iRecord, iFetchPlan);
	}

	public <RET extends ORecordInternal<?>> RET load(final ORecordInternal<?> iRecord, final String iFetchPlan,
			final boolean iIgnoreCache) {
		return (RET) underlying.load(iRecord, iFetchPlan, iIgnoreCache);
	}

	public void reload(final ORecordInternal<?> iRecord) {
		underlying.reload(iRecord, null, true);
	}

	public void reload(final ORecordInternal<?> iRecord, final String iFetchPlan, final boolean iIgnoreCache) {
		underlying.reload(iRecord, iFetchPlan, iIgnoreCache);
	}

	public ODatabaseComplex<ORecordInternal<?>> save(final ORecordInternal<?> iRecord, final String iClusterName) {
		underlying.save(iRecord, iClusterName);
		return this;
	}

	public ODatabaseComplex<ORecordInternal<?>> save(final ORecordInternal<?> iRecord) {
		underlying.save(iRecord);
		return this;
	}

	public void setInternal(final ATTRIBUTES attribute, final Object iValue) {
		underlying.setInternal(attribute, iValue);
	}

	public boolean isRetainRecords() {
		return underlying.isRetainRecords();
	}

	public ODatabaseRecord setRetainRecords(boolean iValue) {
		underlying.setRetainRecords(iValue);
		return (ODatabaseRecord) this;
	}

	public ORecordInternal<?> getRecordByUserObject(final Object iUserObject, final boolean iCreateIfNotAvailable) {
		if (databaseOwner != this)
			return getDatabaseOwner().getRecordByUserObject(iUserObject, false);

		return (ORecordInternal<?>) iUserObject;
	}

	public void registerUserObject(final Object iObject, final ORecordInternal<?> iRecord) {
		if (databaseOwner != this)
			getDatabaseOwner().registerUserObject(iObject, iRecord);
	}

	public Object getUserObjectByRecord(final ORecordInternal<?> iRecord, final String iFetchPlan) {
		if (databaseOwner != this)
			return databaseOwner.getUserObjectByRecord(iRecord, iFetchPlan);

		return iRecord;
	}

	public boolean existsUserObjectByRID(final ORID iRID) {
		if (databaseOwner != this)
			return databaseOwner.existsUserObjectByRID(iRID);
		return false;
	}

	public <DBTYPE extends ODatabaseRecord> DBTYPE checkSecurity(final String iResource, final int iOperation) {
		return (DBTYPE) underlying.checkSecurity(iResource, iOperation);
	}

	public <DBTYPE extends ODatabaseRecord> DBTYPE checkSecurity(final String iResourceGeneric, final int iOperation,
			final Object iResourceSpecific) {
		return (DBTYPE) underlying.checkSecurity(iResourceGeneric, iOperation, iResourceSpecific);
	}

	public <DBTYPE extends ODatabaseRecord> DBTYPE checkSecurity(final String iResourceGeneric, final int iOperation,
			final Object... iResourcesSpecific) {
		return (DBTYPE) underlying.checkSecurity(iResourceGeneric, iOperation, iResourcesSpecific);
	}

	public <DBTYPE extends ODatabaseComplex<?>> DBTYPE registerHook(final ORecordHook iHookImpl) {
		underlying.registerHook(iHookImpl);
		return (DBTYPE) this;
	}

	public boolean callbackHooks(final TYPE iType, final OIdentifiable iObject) {
		return underlying.callbackHooks(iType, iObject);
	}

	public Set<ORecordHook> getHooks() {
		return underlying.getHooks();
	}

	public <DBTYPE extends ODatabaseComplex<?>> DBTYPE unregisterHook(final ORecordHook iHookImpl) {
		underlying.unregisterHook(iHookImpl);
		return (DBTYPE) this;
	}

	protected void checkClusterBoundedToClass(int iClusterId) {
		for (OClass clazz : getMetadata().getSchema().getClasses()) {
			if (clazz.getDefaultClusterId() == iClusterId)
				throw new OSchemaException("Can't drop the cluster '" + getClusterNameById(iClusterId) + "' because the classes ['"
						+ clazz.getName() + "'] are bound to it. Drop these classes before to drop the cluster");
			else if (clazz.getClusterIds().length > 1) {
				for (int i : clazz.getClusterIds()) {
					if (i == iClusterId)
						throw new OSchemaException("Can't drop the cluster '" + getClusterNameById(iClusterId) + "' because the classes ['"
								+ clazz.getName() + "'] are bound to it. Drop these classes before to drop the cluster");
				}
			}
		}
	}
}

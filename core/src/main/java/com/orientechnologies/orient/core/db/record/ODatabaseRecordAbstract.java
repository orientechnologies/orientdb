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
package com.orientechnologies.orient.core.db.record;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.cache.OLevel1RecordCache;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestInternal;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseWrapperAbstract;
import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.hook.OHookThreadLocal;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.hook.ORecordHook.TYPE;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OPropertyIndexManager;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.metadata.security.OUserTrigger;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.query.OQueryAbstract;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordFactory;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;

@SuppressWarnings("unchecked")
public abstract class ODatabaseRecordAbstract extends ODatabaseWrapperAbstract<ODatabaseRaw> implements ODatabaseRecord {

	private OMetadata														metadata;
	private OUser																user;
	private static final String									DEF_RECORD_FORMAT	= "csv";
	private Class<? extends ORecordInternal<?>>	recordClass;
	private String															recordFormat;
	private Set<ORecordHook>										hooks							= new HashSet<ORecordHook>();
	private boolean															retainRecords			= true;
	private OLevel1RecordCache									level1Cache;
	private boolean															mvcc;
	private ODictionary<ORecordInternal<?>>			dictionary;

	public ODatabaseRecordAbstract(final String iURL, final Class<? extends ORecordInternal<?>> iRecordClass) {
		super(new ODatabaseRaw(iURL));
		underlying.setOwner(this);

		databaseOwner = this;

		try {
			recordClass = iRecordClass;
		} catch (Throwable t) {
			throw new ODatabaseException("Error on opening database '" + getName() + "'", t);
		}
		level1Cache = new OLevel1RecordCache(this);

		mvcc = OGlobalConfiguration.DB_MVCC.getValueAsBoolean();

		setCurrentDatabaseinThreadLocal();
	}

	@Override
	public <DB extends ODatabase> DB open(final String iUserName, final String iUserPassword) {
		setCurrentDatabaseinThreadLocal();

		try {
			super.open(iUserName, iUserPassword);
			level1Cache.startup();

			metadata = new OMetadata(this);
			metadata.load();

			recordFormat = DEF_RECORD_FORMAT;

			user = getMetadata().getSecurity().authenticate(iUserName, iUserPassword);

			if (getStorage() instanceof OStorageEmbedded) {
				registerHook(new OUserTrigger());
				registerHook(new OPropertyIndexManager());
			}

			checkSecurity(ODatabaseSecurityResources.DATABASE, ORole.PERMISSION_READ);
		} catch (OException e) {
			close();
			throw e;
		} catch (Exception e) {
			close();
			throw new ODatabaseException("Can't open database", e);
		}
		return (DB) this;
	}

	@Override
	public <DB extends ODatabase> DB create() {
		setCurrentDatabaseinThreadLocal();

		try {
			super.create();

			level1Cache.startup();

			getStorage().getConfiguration().update();

			if (getStorage() instanceof OStorageEmbedded) {
				registerHook(new OUserTrigger());
				registerHook(new OPropertyIndexManager());
			}

			// CREATE THE DEFAULT SCHEMA WITH DEFAULT USER
			metadata = new OMetadata(this);
			metadata.create();

			user = getMetadata().getSecurity().getUser(OUser.ADMIN);
		} catch (Exception e) {
			throw new ODatabaseException("Can't create database", e);
		}
		return (DB) this;
	}

	@Override
	public void close() {
		setCurrentDatabaseinThreadLocal();

		if (metadata != null) {
			metadata.close();
			metadata = null;
		}

		super.close();

		hooks.clear();
		dictionary = null;

		user = null;
		level1Cache.shutdown();
	}

	public ODictionary<ORecordInternal<?>> getDictionary() {
		checkOpeness();
		if (dictionary == null)
			dictionary = metadata.getIndexManager().getDictionary();
		return dictionary;
	}

	public <RET extends ORecordInternal<?>> RET getRecord(final OIdentifiable iIdentifiable) {
		if (iIdentifiable instanceof ORecord<?>)
			return (RET) iIdentifiable;
		return (RET) load(iIdentifiable.getIdentity());
	}

	public <RET extends ORecordInternal<?>> RET load(final ORecordInternal<?> iRecord) {
		return (RET) load(iRecord, null);
	}

	public void reload(final ORecordInternal<?> iRecord) {
		executeReadRecord((ORecordId) iRecord.getIdentity(), iRecord, null, true);
	}

	public void reload(final ORecordInternal<?> iRecord, final String iFetchPlan) {
		executeReadRecord((ORecordId) iRecord.getIdentity(), iRecord, iFetchPlan, true);
	}

	public void reload(final ORecordInternal<?> iRecord, final String iFetchPlan, boolean iIgnoreCache) {
		executeReadRecord((ORecordId) iRecord.getIdentity(), iRecord, iFetchPlan, iIgnoreCache);
	}

	/**
	 * Loads a record using a fetch plan.
	 */
	public <RET extends ORecordInternal<?>> RET load(final ORecordInternal<?> iRecord, final String iFetchPlan) {
		return (RET) executeReadRecord((ORecordId) iRecord.getIdentity(), iRecord, iFetchPlan, false);
	}

	public <RET extends ORecordInternal<?>> RET load(final ORecordInternal<?> iRecord, final String iFetchPlan,
			final boolean iIgnoreCache) {
		return (RET) executeReadRecord((ORecordId) iRecord.getIdentity(), iRecord, iFetchPlan, iIgnoreCache);
	}

	public <RET extends ORecordInternal<?>> RET load(final ORID iRecordId) {
		return (RET) executeReadRecord((ORecordId) iRecordId, null, null, false);
	}

	public <RET extends ORecordInternal<?>> RET load(final ORID iRecordId, final String iFetchPlan) {
		return (RET) executeReadRecord((ORecordId) iRecordId, null, iFetchPlan, false);
	}

	public <RET extends ORecordInternal<?>> RET load(final ORID iRecordId, final String iFetchPlan, final boolean iIgnoreCache) {
		return (RET) executeReadRecord((ORecordId) iRecordId, null, iFetchPlan, iIgnoreCache);
	}

	/**
	 * Update the record without checking the version.
	 */
	public ODatabaseRecord save(final ORecordInternal<?> iContent) {
		executeSaveRecord(iContent, null, iContent.getVersion(), iContent.getRecordType());
		return this;
	}

	/**
	 * Update the record in the requested cluster without checking the version.
	 */
	public ODatabaseRecord save(final ORecordInternal<?> iContent, final String iClusterName) {
		executeSaveRecord(iContent, iClusterName, iContent.getVersion(), iContent.getRecordType());
		return this;
	}

	/**
	 * Delete the record without checking the version.
	 */
	public ODatabaseRecord delete(final ORecordInternal<?> iRecord) {
		executeDeleteRecord(iRecord, -1);
		return this;
	}

	public <REC extends ORecordInternal<?>> ORecordIteratorCluster<REC> browseCluster(final String iClusterName,
			final Class<REC> iClass) {
		checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, iClusterName);

		setCurrentDatabaseinThreadLocal();

		final int clusterId = getClusterIdByName(iClusterName);

		return new ORecordIteratorCluster<REC>(this, this, clusterId);
	}

	public ORecordIteratorCluster<?> browseCluster(final String iClusterName) {
		checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, iClusterName);

		setCurrentDatabaseinThreadLocal();

		final int clusterId = getClusterIdByName(iClusterName);

		return new ORecordIteratorCluster<ORecordInternal<?>>(this, this, clusterId);
	}

	public OCommandRequest command(final OCommandRequest iCommand) {
		setCurrentDatabaseinThreadLocal();

		final OCommandRequestInternal command = (OCommandRequestInternal) iCommand;

		try {
			command.reset();
			command.setDatabase(this);

			return command;

		} catch (Exception e) {
			throw new ODatabaseException("Error on command execution", e);
		}
	}

	public <RET extends List<?>> RET query(final OQuery<? extends Object> iCommand, final Object... iArgs) {
		setCurrentDatabaseinThreadLocal();

		iCommand.reset();

		if (iCommand instanceof OQueryAbstract)
			((OQueryAbstract<?>) iCommand).setDatabase(this);

		return (RET) iCommand.execute(iArgs);
	}

	public Class<? extends ORecordInternal<?>> getRecordType() {
		return recordClass;
	}

	public <RET extends Object> RET newInstance() {
		if (recordClass == null)
			throw new OConfigurationException("Can't create record since no record class was specified");

		return (RET) ORecordFactory.instance().newInstance(this, recordClass);
	}

	public ORecordInternal<?> newInstance(final Class<ORecordInternal<?>> iType) {
		return (ORecordInternal<?>) ORecordFactory.instance().newInstance(this, iType);
	}

	@Override
	public long countClusterElements(final int[] iClusterIds) {
		String name;
		for (int i = 0; i < iClusterIds.length; ++i) {
			name = getClusterNameById(iClusterIds[i]);
			checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, name);
		}

		return super.countClusterElements(iClusterIds);
	}

	@Override
	public long countClusterElements(final int iClusterId) {
		final String name = getClusterNameById(iClusterId);
		checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, name);
		setCurrentDatabaseinThreadLocal();
		return super.countClusterElements(name);
	}

	@Override
	public long countClusterElements(final String iClusterName) {
		checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, iClusterName);
		setCurrentDatabaseinThreadLocal();
		return super.countClusterElements(iClusterName);
	}

	public OMetadata getMetadata() {
		checkOpeness();
		return metadata;
	}

	public <DB extends ODatabaseRecord> DB checkSecurity(final String iResource, final int iOperation) {
		if (user != null) {
			try {
				user.allow(iResource, iOperation);
			} catch (OSecurityAccessException e) {

				if (OLogManager.instance().isDebugEnabled())
					OLogManager.instance().debug(this,
							"[checkSecurity] User '%s' tried to access to the reserved resource '%s', operation '%s'", getUser(), iResource,
							iOperation);

				throw e;
			}
		}
		return (DB) this;
	}

	public <DB extends ODatabaseRecord> DB checkSecurity(final String iResourceGeneric, final int iOperation,
			final Object... iResourcesSpecific) {

		if (user != null) {
			try {
				final StringBuilder keyBuffer = new StringBuilder();

				boolean ruleFound = false;
				for (Object target : iResourcesSpecific) {
					if (target != null) {
						keyBuffer.setLength(0);
						keyBuffer.append(iResourceGeneric);
						keyBuffer.append('.');
						keyBuffer.append(target.toString());

						final String key = keyBuffer.toString();

						if (user.isRuleDefined(key)) {
							ruleFound = true;
							// RULE DEFINED: CHECK AGAINST IT
							user.allow(key, iOperation);
						}
					}
				}

				if (!ruleFound) {
					// CHECK AGAINST GENERIC RULE
					keyBuffer.setLength(0);
					keyBuffer.append(iResourceGeneric);
					keyBuffer.append('.');
					keyBuffer.append(ODatabaseSecurityResources.ALL);

					user.allow(keyBuffer.toString(), iOperation);
				}

			} catch (OSecurityAccessException e) {
				if (OLogManager.instance().isDebugEnabled())
					OLogManager.instance().debug(this,
							"[checkSecurity] User '%s' tried to access to the reserved resource '%s', target(s) '%s', operation '%s'", getUser(),
							iResourceGeneric, Arrays.toString(iResourcesSpecific), iOperation);

				throw e;
			}
		}
		return (DB) this;
	}

	public <DB extends ODatabaseRecord> DB checkSecurity(final String iResourceGeneric, final int iOperation,
			final Object iResourceSpecific) {

		if (user != null) {
			try {
				final StringBuilder keyBuffer = new StringBuilder();

				boolean ruleFound = false;
				if (iResourceSpecific != null) {
					keyBuffer.setLength(0);
					keyBuffer.append(iResourceGeneric);
					keyBuffer.append('.');
					keyBuffer.append(iResourceSpecific.toString());

					final String key = keyBuffer.toString();

					if (user.isRuleDefined(key)) {
						ruleFound = true;
						// RULE DEFINED: CHECK AGAINST IT
						user.allow(key, iOperation);
					}
				}

				if (!ruleFound) {
					// CHECK AGAINST GENERIC RULE
					keyBuffer.setLength(0);
					keyBuffer.append(iResourceGeneric);
					keyBuffer.append('.');
					keyBuffer.append(ODatabaseSecurityResources.ALL);

					user.allow(keyBuffer.toString(), iOperation);
				}

			} catch (OSecurityAccessException e) {
				if (OLogManager.instance().isDebugEnabled())
					OLogManager.instance().debug(this,
							"[checkSecurity] User '%s' tried to access to the reserved resource '%s', target '%s', operation '%s'", getUser(),
							iResourceGeneric, iResourceSpecific, iOperation);

				throw e;
			}
		}
		return (DB) this;
	}

	public <RET extends ORecordInternal<?>> RET executeReadRecord(final ORecordId iRid, ORecordInternal<?> iRecord,
			final String iFetchPlan, final boolean iIgnoreCache) {
		checkOpeness();

		setCurrentDatabaseinThreadLocal();

		try {
			checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, getClusterNameById(iRid.getClusterId()));

			// SEARCH IN LOCAL TX
			ORecordInternal<?> record = getTransaction().getRecordEntry(iRid);
			if (record == null && !iIgnoreCache)
				// SEARCH INTO THE CACHE
				record = getLevel1Cache().findRecord(iRid);

			if (record != null) {
				OFetchHelper.checkFetchPlanValid(iFetchPlan);
				callbackHooks(TYPE.BEFORE_READ, record);

				if (record.getInternalStatus() == ORecordElement.STATUS.NOT_LOADED)
					record.reload();

				callbackHooks(TYPE.AFTER_READ, record);
				return (RET) record;
			}

			final ORawBuffer recordBuffer = underlying.read(iRid, iFetchPlan);
			if (recordBuffer == null)
				return null;

			if (iRecord == null || iRecord.getRecordType() != recordBuffer.recordType)
				// NO SAME RECORD TYPE: CAN'T REUSE OLD ONE BUT CREATE A NEW ONE FOR IT
				iRecord = ORecordFactory.newInstance(recordBuffer.recordType);

			ODatabaseRecord currDb = iRecord.getDatabase();
			if (currDb == null)
				currDb = (ODatabaseRecord) databaseOwner;

			iRecord.fill(currDb, iRid, recordBuffer.version, recordBuffer.buffer, false);

			callbackHooks(TYPE.BEFORE_READ, iRecord);

			iRecord.fromStream(recordBuffer.buffer);
			iRecord.setInternalStatus(ORecordElement.STATUS.LOADED);

			callbackHooks(TYPE.AFTER_READ, iRecord);

			if (!iIgnoreCache) {
				getLevel1Cache().updateRecord(iRecord);
			}

			return (RET) iRecord;
		} catch (OException e) {
			// RE-THROW THE EXCEPTION
			throw e;

		} catch (Exception e) {
			// WRAP IT AS ODATABASE EXCEPTION
			OLogManager.instance().exception("Error on retrieving record " + iRid, e, ODatabaseException.class);
		}
		return null;
	}

	public void executeSaveRecord(final ORecordInternal<?> iRecord, final String iClusterName, final int iVersion,
			final byte iRecordType) {
		checkOpeness();

		if (!iRecord.isDirty())
			return;

		final ORecordId rid = (ORecordId) iRecord.getIdentity();

		if (rid == null)
			throw new ODatabaseException(
					"Can't create record because it has no identity. Probably is not a regular record or contains projections of fields rather than a full record");

		setCurrentDatabaseinThreadLocal();

		if (iRecord.getDatabase() == null)
			iRecord.setDatabase(this);

		try {
			boolean wasNew = rid.isNew();

			// STREAM.LENGTH == 0 -> RECORD IN STACK: WILL BE SAVED AFTER
			byte[] stream = iRecord.toStream();

			boolean isNew = rid.isNew();
			if (isNew)
				// NOTIFY IDENTITY HAS CHANGED
				iRecord.onBeforeIdentityChanged(rid);
			else if (stream.length == 0)
				// ALREADY CREATED AND WAITING FOR THE RIGHT UPDATE (WE'RE IN A GRAPH)
				return;

			if (isNew && rid.clusterId < 0)
				rid.clusterId = iClusterName != null ? getClusterIdByName(iClusterName) : getDefaultClusterId();

			if (stream != null && stream.length > 0) {
				if (wasNew) {
					// CHECK ACCESS ON CLUSTER
					checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_CREATE, iClusterName);
					if (callbackHooks(TYPE.BEFORE_CREATE, iRecord))
						// RECORD CHANGED IN TRIGGER, REACQUIRE IT
						stream = iRecord.toStream();
				} else {
					// CHECK ACCESS ON CLUSTER
					checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_UPDATE, iClusterName);
					if (callbackHooks(TYPE.BEFORE_UPDATE, iRecord))
						// RECORD CHANGED IN TRIGGER, REACQUIRE IT
						stream = iRecord.toStream();
				}

				if (!iRecord.isDirty()) {
					// RECORD SAVED DURING PREVIOUS STREAMING PHASE: THIS HAPPENS FOR CIRCULAR REFERENCED RECORDS
					// ADD/UPDATE IT IN CACHE IF IT'S ACTIVE
					getLevel1Cache().updateRecord(iRecord);
					return;
				}
			}

			// GET THE LATEST VERSION. IT COULD CHANGE BECAUSE THE RECORD COULD BE BEEN LINKED FROM OTHERS
			final int realVersion = iVersion == -1 || !mvcc ? -1 : iRecord.getVersion();

			// SAVE IT
			final long result = underlying.save(rid, stream, realVersion, iRecord.getRecordType());

			if (isNew) {
				// UPDATE INFORMATION: CLUSTER ID+POSITION
				((ORecordId) iRecord.getIdentity()).copyFrom(rid);
				// NOTIFY IDENTITY HAS CHANGED
				iRecord.onAfterIdentityChanged(iRecord);
			}

			if (stream != null && stream.length > 0)
				callbackHooks(wasNew ? TYPE.AFTER_CREATE : TYPE.AFTER_UPDATE, iRecord);

			if (isNew)
				// UPDATE INFORMATION: CLUSTER ID+POSITION
				iRecord.fill(iRecord.getDatabase(), rid, 0, stream, stream == null || stream.length == 0);
			else
				// UPDATE INFORMATION: VERSION
				iRecord.fill(iRecord.getDatabase(), rid, (int) result, stream, stream == null || stream.length == 0);

			if (stream != null && stream.length > 0)
				// ADD/UPDATE IT IN CACHE IF IT'S ACTIVE
				getLevel1Cache().updateRecord(iRecord);

		} catch (OException e) {
			// RE-THROW THE EXCEPTION
			throw e;

		} catch (Throwable t) {
			// WRAP IT AS ODATABASE EXCEPTION
			throw new ODatabaseException("Error on saving record in cluster #" + iRecord.getIdentity().getClusterId(), t);
		}
	}

	public void executeDeleteRecord(final OIdentifiable iRecord, final int iVersion) {
		checkOpeness();
		final ORecordId rid = (ORecordId) iRecord.getIdentity();

		if (rid == null)
			throw new ODatabaseException(
					"Can't delete record because it has no identity. Probably was created from scratch or contains projections of fields rather than a full record");

		if (!rid.isValid())
			return;

		checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_DELETE, getClusterNameById(rid.clusterId));

		setCurrentDatabaseinThreadLocal();

		try {
			callbackHooks(TYPE.BEFORE_DELETE, iRecord);

			underlying.delete(rid, iVersion);

			callbackHooks(TYPE.AFTER_DELETE, iRecord);

			// REMOVE THE RECORD FROM 1 AND 2 LEVEL CACHES
			getLevel1Cache().deleteRecord(rid);

		} catch (OException e) {
			// RE-THROW THE EXCEPTION
			throw e;

		} catch (Throwable t) {
			// WRAP IT AS ODATABASE EXCEPTION
			throw new ODatabaseException("Error on deleting record in cluster #" + iRecord.getIdentity().getClusterId(), t);
		}
	}

	@Override
	public ODatabaseComplex<?> getDatabaseOwner() {
		ODatabaseComplex<?> current = databaseOwner;

		while (current != null && current != this && current.getDatabaseOwner() != current)
			current = current.getDatabaseOwner();

		return current;
	}

	@Override
	public ODatabaseComplex<ORecordInternal<?>> setDatabaseOwner(ODatabaseComplex<?> iOwner) {
		databaseOwner = iOwner;
		return this;
	}

	public boolean isRetainRecords() {
		return retainRecords;
	}

	public ODatabaseRecord setRetainRecords(boolean retainRecords) {
		this.retainRecords = retainRecords;
		return this;
	}

	public <DB extends ODatabase> DB setStatus(final STATUS status) {
		final String cmd = String.format("alter database status %s", status.toString());
		command(new OCommandSQL(cmd)).execute();
		return (DB) this;
	}

	public void setStatusInternal(final STATUS status) {
		underlying.setStatus(status);
	}

	public void setInternal(final ATTRIBUTES iAttribute, final Object iValue) {
		if (iAttribute == null)
			throw new IllegalArgumentException("attribute is null");

		final String stringValue = iValue != null ? iValue.toString() : null;

		switch (iAttribute) {
		case STATUS:
			setStatusInternal(STATUS.valueOf(stringValue.toUpperCase()));
			break;
		}
	}

	public OUser getUser() {
		return user;
	}

	public boolean isMVCC() {
		return mvcc;
	}

	public ODatabaseRecord setMVCC(boolean mvcc) {
		this.mvcc = mvcc;
		return this;
	}

	public <DB extends ODatabaseComplex<?>> DB registerHook(final ORecordHook iHookImpl) {
		hooks.add(iHookImpl);
		return (DB) this;
	}

	public <DB extends ODatabaseComplex<?>> DB unregisterHook(final ORecordHook iHookImpl) {
		hooks.remove(iHookImpl);
		return (DB) this;
	}

	@Override
	public OLevel1RecordCache getLevel1Cache() {
		return level1Cache;
	}

	public Set<ORecordHook> getHooks() {
		return Collections.unmodifiableSet(hooks);
	}

	/**
	 * Callback the registeted hooks if any.
	 * 
	 * @param iType
	 * @param iRecord
	 *          Record received in the callback
	 * @return True if the input record is changed, otherwise false
	 */
	public boolean callbackHooks(final TYPE iType, final OIdentifiable iRecord) {
		if (!OHookThreadLocal.INSTANCE.push(iRecord))
			return false;

		try {
			boolean recordChanged = false;
			for (ORecordHook hook : hooks)
				if (hook.onTrigger(iType, (ORecord<?>) iRecord))
					recordChanged = true;
			return recordChanged;

		} finally {
			OHookThreadLocal.INSTANCE.pop(iRecord);
		}
	}

	protected ORecordSerializer resolveFormat(final Object iObject) {
		return ORecordSerializerFactory.instance().getFormatForObject(iObject, recordFormat);
	}

	@Override
	protected void checkOpeness() {
		if (isClosed())
			throw new ODatabaseException("Database is closed");
	}

	protected void setCurrentDatabaseinThreadLocal() {
		ODatabaseRecordThreadLocal.INSTANCE.set(this);
	}
}

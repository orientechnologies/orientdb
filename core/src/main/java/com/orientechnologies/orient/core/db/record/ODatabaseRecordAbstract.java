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
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestInternal;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseWrapperAbstract;
import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.dictionary.ODictionaryInternal;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.ORecordFactory;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecord.STATUS;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.memory.OStorageMemory;

@SuppressWarnings("unchecked")
public abstract class ODatabaseRecordAbstract<REC extends ORecordInternal<?>> extends ODatabaseWrapperAbstract<ODatabaseRaw, REC>
		implements ODatabaseRecord<REC> {

	private ODictionaryInternal		dictionary;
	private OMetadata							metadata;
	private OUser									user;
	private static final String		DEF_RECORD_FORMAT	= "csv";
	private Class<? extends REC>	recordClass;
	private String								recordFormat;
	private Set<ORecordHook>			hooks							= new HashSet<ORecordHook>();

	public ODatabaseRecordAbstract(final String iURL, final Class<? extends REC> iRecordClass) {
		super(new ODatabaseRaw(iURL));
		underlying.setOwner(this);

		databaseOwner = this;

		try {
			recordClass = iRecordClass;

			metadata = new OMetadata(this);
			dictionary = (ODictionaryInternal) getStorage().createDictionary(this);
		} catch (Throwable t) {
			throw new ODatabaseException("Error on opening database '" + getName() + "'", t);
		}
	}

	@Override
	public <DB extends ODatabase> DB open(final String iUserName, final String iUserPassword) {
		try {
			super.open(iUserName, iUserPassword);

			metadata.load();

			recordFormat = DEF_RECORD_FORMAT;
			dictionary.load();

			if (!(getStorage() instanceof OStorageMemory)) {
				user = getMetadata().getSecurity().getUser(iUserName);
				if (user == null)
					throw new OSecurityAccessException(this.getName(), "User '" + iUserName + "' was not found in database: " + getName());

				if (!user.checkPassword(iUserPassword)) {
					// WAIT A BIT TO AVOID BRUTE FORCE
					Thread.sleep(200);
					throw new OSecurityAccessException(this.getName(), "Password not valid for user: " + iUserName);
				}
			}

			checkSecurity(ODatabaseSecurityResources.DATABASE, ORole.PERMISSION_READ);
		} catch (Exception e) {
			close();
			throw new ODatabaseException("Can't open database", e);
		}
		return (DB) this;
	}

	@Override
	public <DB extends ODatabase> DB create() {
		try {
			super.create();

			// CREATE THE DEFAULT SCHEMA WITH DEFAULT USER
			metadata.create();

			createRolesAndUsers();

			dictionary.create();
		} catch (Exception e) {
			throw new ODatabaseException("Can't create database", e);
		}
		return (DB) this;
	}

	@Override
	public void close() {
		super.close();
		user = null;
	}

	public REC load(final REC iRecord) {
		return load(iRecord.getIdentity().getClusterId(), iRecord.getIdentity().getClusterPosition(), iRecord);
	}

	public REC load(final ORID iRecordId) {
		return load(iRecordId.getClusterId(), iRecordId.getClusterPosition(), (REC) databaseOwner.newInstance());
	}

	public REC load(final int iClusterId, final long iPosition, final REC iRecord) {
		return executeReadRecord(iClusterId, iPosition, iRecord);
	}

	/**
	 * Update the record without checking the version.
	 */
	public ODatabaseRecord<REC> save(final REC iContent) {
		executeSaveRecord(iContent, null, iContent.getVersion(), iContent.getRecordType());
		return this;
	}

	/**
	 * Update the record in the requested cluster without checking the version.
	 */
	public ODatabaseRecord<REC> save(final REC iContent, final String iClusterName) {
		executeSaveRecord(iContent, iClusterName, iContent.getVersion(), iContent.getRecordType());
		return this;
	}

	/**
	 * Delete the record without checking the version.
	 */
	public ODatabaseRecord<REC> delete(final REC iRecord) {
		executeDeleteRecord(iRecord, -1);
		return this;
	}

	public ORecordIteratorCluster<REC> browseCluster(String iClusterName) {
		final int clusterId = getClusterIdByName(iClusterName);

		checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, iClusterName, clusterId);

		return new ORecordIteratorCluster<REC>(this, this, clusterId);
	}

	public OCommandRequest command(final OCommandRequest iCommand) {
		OCommandRequestInternal command = (OCommandRequestInternal) iCommand;

		try {
			command.setDatabase(getDatabaseOwner());

			return command;

		} catch (Exception e) {
			throw new ODatabaseException("Error on command execution", e);
		}
	}

	public Class<? extends REC> getRecordType() {
		return recordClass;
	}

	public REC newInstance() {
		if (recordClass == null)
			throw new OConfigurationException("Can't create record since no record class was specified");

		return (REC) ORecordFactory.instance().newInstance(this, recordClass);
	}

	public REC newInstance(final Class<REC> iType) {
		return (REC) ORecordFactory.instance().newInstance(this, iType);
	}

	@Override
	public long countClusterElements(final int[] iClusterIds) {
		String name;
		for (int i = 0; i < iClusterIds.length; ++i) {
			name = getClusterNameById(iClusterIds[i]);
			checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, name, iClusterIds[i]);
		}

		return super.countClusterElements(iClusterIds);
	}

	@Override
	public long countClusterElements(final int iClusterId) {
		final String name = getClusterNameById(iClusterId);
		checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, name, iClusterId);
		return super.countClusterElements(name);
	}

	@Override
	public long countClusterElements(final String iClusterName) {
		final int clusterId = getClusterIdByName(iClusterName);
		checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, iClusterName, clusterId);
		return super.countClusterElements(iClusterName);
	}

	public OMetadata getMetadata() {
		return metadata;
	}

	public ODictionary<REC> getDictionary() {
		checkOpeness();
		return dictionary;
	}

	public <DB extends ODatabaseRecord<?>> DB checkSecurity(final String iResource, final int iOperation) {
		OLogManager.instance()
				.debug(this, "[checkSecurity] Check permissions for resource '%s', operation '%s'", iResource, iOperation);
		if (user != null) {
			try {
				final ORole role = user.allow(iResource, iOperation);
				OLogManager.instance().debug(this, "[checkSecurity] Granted permission for resource '%s', operation '%s' by role: %s",
						iResource, iOperation, role.getName());

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

	public <DB extends ODatabaseRecord<?>> DB checkSecurity(final String iResourceGeneric, final int iOperation,
			final Object... iResourcesSpecific) {

		if (OLogManager.instance().isDebugEnabled())
			OLogManager.instance().debug(this, "[checkSecurity] Check permissions for resource '%s', target(s) '%s', operation '%s'",
					iResourceGeneric, Arrays.toString(iResourcesSpecific), iOperation);

		if (user != null) {
			try {
				ORole role = user.allow(iResourceGeneric + "." + ODatabaseSecurityResources.ALL, iOperation);

				for (Object target : iResourcesSpecific)
					if (target != null && user.isRuleDefined(iResourceGeneric + "." + target.toString())) {
						// RULE DEFINED: CHECK AGAINST IT
						role = user.allow(iResourceGeneric + "." + target.toString(), iOperation);
					}

				if (OLogManager.instance().isDebugEnabled())
					OLogManager.instance().debug(this,
							"[checkSecurity] Granted permission for resource '%s', target(s) '%s', operation '%s' by role: %s", iResourceGeneric,
							Arrays.toString(iResourcesSpecific), iOperation, role.getName());

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

	public REC executeReadRecord(final int iClusterId, final long iPosition, REC iRecord) {
		checkOpeness();

		try {
			checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_READ, getClusterNameById(iClusterId), iClusterId);

			final ORawBuffer recordBuffer = underlying.read(iClusterId, iPosition);
			if (recordBuffer == null)
				return null;

			if (iRecord.getRecordType() != recordBuffer.recordType)
				// NO SAME RECORD TYPE: CAN'T REUSE OLD ONE BUT CREATE A NEW ONE FOR IT
				iRecord = (REC) ORecordFactory.getRecord(recordBuffer.recordType);

			iRecord.unsetDirty();

			ODatabaseRecord<?> currDb = iRecord.getDatabase();
			if (currDb == null)
				currDb = (ODatabaseRecord<?>) databaseOwner;

			iRecord.fill(currDb, iClusterId, iPosition, recordBuffer.version);
			iRecord.fromStream(recordBuffer.buffer);
			iRecord.setStatus(STATUS.LOADED);

			return iRecord;
		} catch (ODatabaseException e) {
			// RE-THROW THE EXCEPTION
			throw e;

		} catch (Exception e) {
			// WRAP IT AS ODATABASE EXCEPTION
			OLogManager.instance().exception("Error on retrieving record #%d in cluster '%s'", e, ODatabaseException.class, iPosition,
					getStorage().getPhysicalClusterNameById(iClusterId));
		}
		return null;
	}

	public void executeSaveRecord(final REC iContent, final String iClusterName, final int iVersion, final byte iRecordType) {
		checkOpeness();

		if (!iContent.isDirty())
			return;

		final ORecordId rid = (ORecordId) iContent.getIdentity();

		if (rid == null)
			throw new ODatabaseException(
					"Can't create record because it has no identity. Probably is not a regular record or contains projections of fields rather than a full record");

		try {
			final int clusterId;

			boolean isNew = !rid.isValid();
			if (isNew) {
				clusterId = iClusterName != null ? getClusterIdByName(iClusterName) : getDefaultClusterId();

				// CHECK ACCESS ON CLUSTER
				checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_CREATE, iClusterName, clusterId);
			} else {
				clusterId = rid.clusterId;

				// CHECK ACCESS ON CLUSTER
				checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_UPDATE, iClusterName, clusterId);
			}

			final byte[] stream = iContent.toStream();

			// SAVE IT
			long result = underlying.save(clusterId, rid.getClusterPosition(), stream, iVersion, iContent.getRecordType());

			if (isNew)
				// UPDATE INFORMATION: CLUSTER ID+POSITION
				iContent.fill(iContent.getDatabase(), clusterId, result, 0);
			else
				// UPDATE INFORMATION: VERSION
				iContent.fill(iContent.getDatabase(), clusterId, rid.getClusterPosition(), (int) result);

			iContent.unsetDirty();
		} catch (ODatabaseException e) {
			// RE-THROW THE EXCEPTION
			throw e;
		} catch (Throwable t) {
			// WRAP IT AS ODATABASE EXCEPTION
			throw new ODatabaseException("Error on saving record in cluster #" + iContent.getIdentity().getClusterId(), t);
		}
	}

	public void executeDeleteRecord(final REC iContent, final int iVersion) {
		checkOpeness();
		ORecordId rid = (ORecordId) iContent.getIdentity();

		if (rid == null)
			throw new ODatabaseException(
					"Can't delete record because it has no identity. Probably was created from scratch or contains projections of fields rather than a full record");

		final int clusterId = iContent.getIdentity().getClusterId();
		checkSecurity(ODatabaseSecurityResources.CLUSTER, ORole.PERMISSION_DELETE, getClusterNameById(clusterId), clusterId);

		try {
			underlying.delete(iContent.getIdentity().getClusterId(), iContent.getIdentity().getClusterPosition(), iVersion);

			// DELETE IT ALSO IN CACHE
			if (underlying.isUseCache())
				getCache().removeRecord(rid.toString());

		} catch (ODatabaseException e) {
			// RE-THROW THE EXCEPTION
			throw e;

		} catch (Throwable t) {
			// WRAP IT AS ODATABASE EXCEPTION
			throw new ODatabaseException("Error on deleting record in cluster #" + iContent.getIdentity().getClusterId(), t);
		}
	}

	@Override
	public ODatabaseRecord<?> getDatabaseOwner() {
		return (ODatabaseRecord<?>) databaseOwner;
	}

	@Override
	public ODatabaseComplex<REC> setDatabaseOwner(ODatabaseComplex<?> iOwner) {
		databaseOwner = iOwner;
		return this;
	}

	protected ORecordSerializer resolveFormat(final Object iObject) {
		return ORecordSerializerFactory.instance().getFormatForObject(iObject, recordFormat);
	}

	@Override
	protected void checkOpeness() {
		if (isClosed())
			throw new ODatabaseException("Database is closed");
	}

	public OUser getUser() {
		return user;
	}

	public <DB extends ODatabaseRecord<?>> DB registerHook(final ORecordHook iHookImpl) {
		hooks.add(iHookImpl);
		return (DB) this;
	}

	public <DB extends ODatabaseRecord<?>> DB unregisterHook(final ORecordHook iHookImpl) {
		hooks.remove(iHookImpl);
		return (DB) this;
	}

	public Set<ORecordHook> getHooks() {
		return Collections.unmodifiableSet(hooks);
	}

	private void createRolesAndUsers() {
		final int metadataClusterId = getClusterIdByName(OStorage.CLUSTER_METADATA_NAME);
		metadata.getSchema().createClass("ORole", metadataClusterId);
		metadata.getSchema().createClass("OUser", metadataClusterId);

		final ORole role = metadata.getSecurity().createRole("admin", ORole.ALLOW_MODES.ALLOW_ALL_BUT);
		user = metadata.getSecurity().createUser("admin", "admin", new String[] { role.getName() });

		final ORole readerRole = metadata.getSecurity().createRole("reader", ORole.ALLOW_MODES.DENY_ALL_BUT);
		readerRole.addRule(ODatabaseSecurityResources.DATABASE, ORole.PERMISSION_READ);
		readerRole.addRule(ODatabaseSecurityResources.CLUSTER + ".metadata", ORole.PERMISSION_NONE);
		readerRole.addRule(ODatabaseSecurityResources.ALL_CLASSES, ORole.PERMISSION_READ);
		readerRole.addRule(ODatabaseSecurityResources.ALL_CLUSTERS, ORole.PERMISSION_READ);
		readerRole.addRule(ODatabaseSecurityResources.QUERY, ORole.PERMISSION_READ);
		readerRole.addRule(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_READ);
		readerRole.addRule(ODatabaseSecurityResources.RECORD_HOOK, ORole.PERMISSION_READ);
		metadata.getSecurity().createUser("reader", "reader", new String[] { readerRole.getName() });

		final ORole writerRole = metadata.getSecurity().createRole("writer", ORole.ALLOW_MODES.DENY_ALL_BUT);
		writerRole.addRule(ODatabaseSecurityResources.DATABASE, ORole.PERMISSION_READ);
		writerRole.addRule(ODatabaseSecurityResources.CLUSTER + ".metadata", ORole.PERMISSION_NONE);
		writerRole.addRule(ODatabaseSecurityResources.ALL_CLASSES, ORole.PERMISSION_ALL);
		writerRole.addRule(ODatabaseSecurityResources.ALL_CLUSTERS, ORole.PERMISSION_ALL);
		writerRole.addRule(ODatabaseSecurityResources.QUERY, ORole.PERMISSION_READ);
		writerRole.addRule(ODatabaseSecurityResources.COMMAND, ORole.PERMISSION_ALL);
		writerRole.addRule(ODatabaseSecurityResources.RECORD_HOOK, ORole.PERMISSION_ALL);
		metadata.getSecurity().createUser("writer", "writer", new String[] { writerRole.getName() });

		metadata.getSecurity().save();
	}
}

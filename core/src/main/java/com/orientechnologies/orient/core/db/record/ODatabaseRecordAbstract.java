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
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.ORecordFactory;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecord.STATUS;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.storage.ORawBuffer;
import com.orientechnologies.orient.core.storage.impl.memory.OStorageMemory;

@SuppressWarnings("unchecked")
public abstract class ODatabaseRecordAbstract<REC extends ORecordInternal<?>> extends ODatabaseWrapperAbstract<ODatabaseRaw, REC>
		implements ODatabaseRecord<REC> {
	private ODictionaryInternal		dictionary;
	private OMetadata							metadata;

	private static final String		DEF_RECORD_FORMAT	= "csv";
	private Class<? extends REC>	recordClass;
	private String								recordFormat;

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

			if (!(getStorage() instanceof OStorageMemory)) {
				user = getMetadata().getSecurity().getUser(iUserName);
				if (user != null && user.checkPassword(iUserPassword)) {
					user = getMetadata().getSecurity().getUser(iUserName);
				} else
					throw new OSecurityAccessException("Error on opening the database. User and/or password are not valid");
			}

			checkSecurity(OUser.DATABASE, OUser.READ);

			recordFormat = DEF_RECORD_FORMAT;
			dictionary.load();
		} catch (Exception e) {
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
			metadata.getSecurity().createUser("admin", "admin", OUser.MODE.ALLOW_ALL_BUT, null);
			metadata.getSecurity().save();

			dictionary.create();
		} catch (Exception e) {
			throw new ODatabaseException("Can't create database", e);
		}
		return (DB) this;
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
		checkSecurity(OUser.CLUSTER + "." + iClusterName, OUser.READ);

		return new ORecordIteratorCluster<REC>(this, this, getClusterIdByName(iClusterName));
	}

	public OCommandRequest command(final OCommandRequest iCommand) {
		checkSecurity(OUser.COMMAND, OUser.READ);

		OCommandRequestInternal command = (OCommandRequestInternal) iCommand;

		try {
			command.setDatabase((ODatabaseRecord<ODocument>) getDatabaseOwner());

			return command;

		} catch (Exception e) {
			throw new ODatabaseException("Error on command execution", e);
		}
	}

	//
	// public OQuery<REC> query(final OQuery<REC> iQuery) {
	// checkSecurity(OUser.QUERY, OUser.READ);
	//
	// OQueryInternal<REC> query = (OQueryInternal<REC>) iQuery;
	//
	// try {
	// query.setDatabase((ODatabaseRecord<REC>) getDatabaseOwner());
	//
	// if (query.getRecord() == null)
	// query.setRecord((REC) getDatabaseOwner().newInstance());
	//
	// return query;
	//
	// } catch (Exception e) {
	// throw new ODatabaseException("Error on query execution", e);
	// }
	// }

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
			checkSecurity(OUser.CLUSTER + "." + name, OUser.READ);
		}

		return super.countClusterElements(iClusterIds);
	}

	@Override
	public long countClusterElements(final int iClusterId) {
		String name = getClusterNameById(iClusterId);
		checkSecurity(OUser.CLUSTER + "." + name, OUser.READ);
		return super.countClusterElements(name);
	}

	@Override
	public long countClusterElements(final String iClusterName) {
		checkSecurity(OUser.CLUSTER + "." + iClusterName, OUser.READ);
		return super.countClusterElements(iClusterName);
	}

	public OMetadata getMetadata() {
		return metadata;
	}

	public ODictionary<REC> getDictionary() {
		checkOpeness();
		return dictionary;
	}

	public REC executeReadRecord(final int iClusterId, final long iPosition, REC iRecord) {
		checkOpeness();

		try {
			checkSecurity(OUser.CLUSTER + "." + iClusterId, OUser.READ);

			final ORawBuffer recordBuffer = underlying.read(iClusterId, iPosition);
			if (recordBuffer == null)
				return null;

			if (iRecord.getRecordType() != recordBuffer.recordType) {
				iRecord = (REC) ORecordFactory.getRecord(recordBuffer.recordType);
			}

			iRecord.unsetDirty();

			ODatabaseRecord<?> currDb = iRecord.getDatabase();
			if (currDb == null)
				currDb = (ODatabaseRecord<?>) databaseOwner;

			iRecord.fill(currDb, iClusterId, iPosition, recordBuffer.version);
			iRecord.fromStream(recordBuffer.buffer);
			iRecord.setStatus(STATUS.LOADED);

			return (REC) iRecord;
		} catch (ODatabaseException e) {
			// RE-THROW THE EXCEPTION
			throw e;

		} catch (Throwable t) {
			// WRAP IT AS ODATABASE EXCEPTION
			OLogManager.instance().error(this,
					"Error on retrieving record #" + iPosition + " in cluster '" + getStorage().getPhysicalClusterNameById(iClusterId) + "'",
					t);
		}
		return null;
	}

	public void executeSaveRecord(final REC iContent, String iClusterName, final int iVersion, final byte iRecordType) {
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
				checkSecurity(OUser.CLUSTER + "." + clusterId, OUser.CREATE);
			} else {
				// if (iClusterName != null)
				// throw new IllegalArgumentException("Can't specify the cluster on update but only on creation");

				clusterId = rid.clusterId;

				// CHECK ACCESS ON CLUSTER
				checkSecurity(OUser.CLUSTER + "." + clusterId, OUser.UPDATE);
			}

			final byte[] stream = iContent.toStream();

			long clusterPosition = underlying.save(clusterId, rid.getClusterPosition(), stream, iVersion, iContent.getRecordType());

			if (clusterPosition < -1)
				iContent.fill(iContent.getDatabase(), rid.getClusterId(), rid.getClusterPosition(), (int) clusterPosition * -1 - 2);

			if (!rid.isValid()) {
				rid.clusterId = clusterId;
				rid.clusterPosition = clusterPosition;
			}

			// ADD/UPDATE IT IN CACHE
			if (underlying.isUseCache())
				getCache().addRecord(rid.toString(), new ORawBuffer(stream, isNew ? 0 : iVersion, iRecordType));

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

		checkSecurity(OUser.CLUSTER + "." + iContent.getIdentity().getClusterId(), OUser.DELETE);

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

	public ODatabaseRecord<?> getDatabaseOwner() {
		return (ODatabaseRecord<?>) databaseOwner;
	}

	public ODatabaseComplex<REC> setDatabaseOwner(ODatabaseComplex<?> iOwner) {
		databaseOwner = (ODatabaseRecord<?>) iOwner;
		return this;
	}

	protected ORecordSerializer resolveFormat(final Object iObject) {
		return ORecordSerializerFactory.instance().getFormatForObject(iObject, recordFormat);
	}

	protected void checkOpeness() {
		if (isClosed())
			throw new ODatabaseException("Database is closed");
	}
}

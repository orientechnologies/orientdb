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
package com.orientechnologies.orient.core.db.object.jpa;

import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.Query;

import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;

public class OJPAEntityManager implements EntityManager {
	private ODatabaseObjectTx	database;
	private FlushModeType			flushMode	= FlushModeType.AUTO;
	private EntityTransaction	transaction;

	public OJPAEntityManager(final Map<?, ?> map) {
		final String url = (String) map.get("url");
		database = new ODatabaseObjectTx(url);
		transaction = new OJPAEntityTransaction(database);
	}

	public void persist(Object entity) {
		database.save(entity);
	}

	public <T> T merge(T entity) {
		throw new UnsupportedOperationException("merge");
	}

	public void remove(Object entity) {
		database.delete(entity);
	}

	@SuppressWarnings("unchecked")
	public <T> T find(Class<T> entityClass, Object primaryKey) {
		final ORecordId rid;

		if (primaryKey instanceof ORecordId)
			rid = (ORecordId) primaryKey;
		else if (primaryKey instanceof String)
			rid = new ORecordId((String) primaryKey);
		else if (primaryKey instanceof Number) {
			// COMPOSE THE RID
			OClass cls = database.getMetadata().getSchema().getClass(entityClass);
			if (cls == null)
				throw new IllegalArgumentException("Class '" + entityClass + "' is not configured in the database");
			rid = new ORecordId(cls.getDefaultClusterId(), ((Number) primaryKey).longValue());
		} else
			throw new IllegalArgumentException("PrimaryKey '" + primaryKey + "' type (" + primaryKey.getClass() + ") is not supported");

		return (T) database.load(rid);
	}

	public <T> T getReference(Class<T> entityClass, Object primaryKey) {
		throw new UnsupportedOperationException("merge");
	}

	public void flush() {
		if (flushMode == FlushModeType.COMMIT)
			database.commit();
	}

	public void setFlushMode(FlushModeType flushMode) {
		this.flushMode = flushMode;
	}

	public FlushModeType getFlushMode() {
		return flushMode;
	}

	public void lock(Object entity, LockModeType lockMode) {
		throw new UnsupportedOperationException("lock");
	}

	public void refresh(Object entity) {
		database.load(entity);
	}

	public void clear() {
		if (flushMode == FlushModeType.COMMIT)
			database.rollback();
	}

	public boolean contains(Object entity) {
		return database.isManaged(entity);
	}

	public Query createQuery(String qlString) {
		throw new UnsupportedOperationException("createQuery");
	}

	public Query createNamedQuery(String name) {
		throw new UnsupportedOperationException("createNamedQuery");
	}

	public Query createNativeQuery(String sqlString) {
		throw new UnsupportedOperationException("createNativeQuery");
	}

	@SuppressWarnings("rawtypes")
	public Query createNativeQuery(String sqlString, Class resultClass) {
		throw new UnsupportedOperationException("createNativeQuery");
	}

	public Query createNativeQuery(String sqlString, String resultSetMapping) {
		throw new UnsupportedOperationException("createNativeQuery");
	}

	public void joinTransaction() {
		throw new UnsupportedOperationException("joinTransaction");
	}

	public Object getDelegate() {
		return database;
	}

	public EntityTransaction getTransaction() {
		return transaction;
	}

	public void close() {
		database.close();
	}

	public boolean isOpen() {
		return !database.isClosed();
	}
}

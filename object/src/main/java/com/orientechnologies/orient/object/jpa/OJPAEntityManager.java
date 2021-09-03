/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.object.jpa;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.FlushModeType;
import javax.persistence.LockModeType;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.metamodel.Metamodel;

public class OJPAEntityManager implements EntityManager {
  /** the log used by this class. */
  private static Logger logger = Logger.getLogger(OJPAPersistenceProvider.class.getName());

  private final EntityManagerFactory emFactory;
  private final OObjectDatabaseTx database;
  private final EntityTransaction transaction;
  private final OJPAProperties properties;
  private FlushModeType flushMode = FlushModeType.AUTO;

  OJPAEntityManager(EntityManagerFactory entityManagerFactory, OJPAProperties properties) {
    this.properties = properties;
    this.emFactory = entityManagerFactory;

    this.database = new OObjectDatabaseTx(properties.getURL());
    database.open(properties.getUser(), properties.getPassword());
    if (properties.isEntityClasses()) {
      database.getEntityManager().registerEntityClasses(properties.getEntityClasses());
    }
    transaction = new OJPAEntityTransaction(database);

    if (logger.isLoggable(Level.INFO)) {
      logger.info(
          "EntityManager created for persistence unit : " + entityManagerFactory.toString());
    }
  }

  @Override
  public void persist(Object entity) {
    database.save(entity);
  }

  @Override
  public <T> T merge(T entity) {
    throw new UnsupportedOperationException("merge");
  }

  @Override
  public void remove(Object entity) {
    database.delete(entity);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T find(Class<T> entityClass, Object primaryKey) {
    final ORecordId rid;

    if (primaryKey instanceof ORecordId) {
      rid = (ORecordId) primaryKey;
    } else if (primaryKey instanceof String) {
      rid = new ORecordId((String) primaryKey);
    } else if (primaryKey instanceof Number) {
      // COMPOSE THE RID
      OClass cls = database.getMetadata().getSchema().getClass(entityClass);
      if (cls == null) {
        throw new IllegalArgumentException(
            "Class '" + entityClass + "' is not configured in the database");
      }
      rid = new ORecordId(cls.getDefaultClusterId(), ((Number) primaryKey).longValue());
    } else {
      throw new IllegalArgumentException(
          "PrimaryKey '" + primaryKey + "' type (" + primaryKey.getClass() + ") is not supported");
    }

    return (T) database.load(rid);
  }

  @Override
  public <T> T getReference(Class<T> entityClass, Object primaryKey) {
    throw new UnsupportedOperationException("merge");
  }

  @Override
  public void flush() {
    if (flushMode == FlushModeType.COMMIT) {
      database.commit();
      if (logger.isLoggable(Level.FINEST)) {
        logger.info("EntityManager flushed. " + toString());
      }
    }
  }

  @Override
  public FlushModeType getFlushMode() {
    return flushMode;
  }

  @Override
  public void setFlushMode(FlushModeType flushMode) {
    this.flushMode = flushMode;
  }

  @Override
  public void lock(Object entity, LockModeType lockMode) {
    throw new UnsupportedOperationException("lock");
  }

  @Override
  public void refresh(Object entity) {
    database.load(entity);
    if (logger.isLoggable(Level.FINEST)) {
      logger.info("EntityManager refreshed. " + toString());
    }
  }

  @Override
  public void clear() {
    if (flushMode == FlushModeType.COMMIT) {
      database.rollback();
      if (logger.isLoggable(Level.FINEST)) {
        logger.info("EntityManager cleared. " + toString());
      }
    }
  }

  @Override
  public boolean contains(Object entity) {
    return database.isManaged(entity);
  }

  @Override
  public Query createQuery(String qlString) {
    throw new UnsupportedOperationException("createQuery");
  }

  @Override
  public Query createNamedQuery(String name) {
    throw new UnsupportedOperationException("createNamedQuery");
  }

  @Override
  public Query createNativeQuery(String sqlString) {
    throw new UnsupportedOperationException("createNativeQuery");
  }

  @Override
  @SuppressWarnings("rawtypes")
  public Query createNativeQuery(String sqlString, Class resultClass) {
    throw new UnsupportedOperationException("createNativeQuery");
  }

  @Override
  public Query createNativeQuery(String sqlString, String resultSetMapping) {
    throw new UnsupportedOperationException("createNativeQuery");
  }

  @Override
  public void joinTransaction() {
    throw new UnsupportedOperationException("joinTransaction");
  }

  @Override
  public <T> T find(Class<T> entityClass, Object primaryKey, Map<String, Object> properties) {
    throw new UnsupportedOperationException("find(Class<T>, LockModeType, Map<String, Object>)");
  }

  @Override
  public <T> T find(Class<T> entityClass, Object primaryKey, LockModeType lockMode) {
    throw new UnsupportedOperationException("find(Class<T>, Object, LockModeType");
  }

  @Override
  public <T> T find(
      Class<T> entityClass,
      Object primaryKey,
      LockModeType lockMode,
      Map<String, Object> properties) {
    throw new UnsupportedOperationException(
        "find(Class<T>, Object, LockModeType, Map<String, Object>)");
  }

  @Override
  public void lock(Object entity, LockModeType lockMode, Map<String, Object> properties) {
    throw new UnsupportedOperationException("lock");
  }

  @Override
  public void refresh(Object entity, Map<String, Object> properties) {
    throw new UnsupportedOperationException("refresh");
  }

  @Override
  public void refresh(Object entity, LockModeType lockMode) {
    throw new UnsupportedOperationException("refresh");
  }

  @Override
  public void refresh(Object entity, LockModeType lockMode, Map<String, Object> properties) {
    throw new UnsupportedOperationException("refresh");
  }

  @Override
  public void detach(Object entity) {
    throw new UnsupportedOperationException("detach");
  }

  @Override
  public LockModeType getLockMode(Object entity) {
    throw new UnsupportedOperationException("getLockMode");
  }

  @Override
  public void setProperty(String propertyName, Object value) {
    throw new UnsupportedOperationException("setProperty");
  }

  @Override
  public Map<String, Object> getProperties() {
    return properties.getUnmodifiableProperties();
  }

  @Override
  public <T> TypedQuery<T> createQuery(CriteriaQuery<T> criteriaQuery) {
    throw new UnsupportedOperationException("createQuery");
  }

  @Override
  public <T> TypedQuery<T> createQuery(String qlString, Class<T> resultClass) {
    throw new UnsupportedOperationException("createQuery");
  }

  @Override
  public <T> TypedQuery<T> createNamedQuery(String name, Class<T> resultClass) {
    throw new UnsupportedOperationException("createNamedQuery");
  }

  @Override
  public <T> T unwrap(Class<T> cls) {
    throw new UnsupportedOperationException("unwrap");
  }

  @Override
  public EntityManagerFactory getEntityManagerFactory() {
    return emFactory;
  }

  @Override
  public CriteriaBuilder getCriteriaBuilder() {
    throw new UnsupportedOperationException("getCriteriaBuilder");
  }

  @Override
  public Metamodel getMetamodel() {
    throw new UnsupportedOperationException("getMetamodel");
  }

  @Override
  public Object getDelegate() {
    return database;
  }

  @Override
  public EntityTransaction getTransaction() {
    return transaction;
  }

  @Override
  public void close() {
    database.close();
    if (logger.isLoggable(Level.INFO)) {
      logger.info("EntityManager closed. " + toString());
    }
  }

  @Override
  public boolean isOpen() {
    return !database.isClosed();
  }

  @Override
  public String toString() {
    return "EntityManager for User@Database:"
        + database.getUser()
        + "@"
        + database.getURL()
        + ", "
        + super.toString();
  }
}

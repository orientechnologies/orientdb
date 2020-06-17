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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.persistence.Cache;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnitUtil;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.metamodel.Metamodel;

/**
 * JPA EntityManagerFactory implementation that uses OrientDB EntityManager instances.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OJPAEntityManagerFactory implements EntityManagerFactory {
  /** the log used by this class. */
  private static Logger logger = Logger.getLogger(OJPAPersistenceProvider.class.getName());

  private boolean opened = true;
  private final List<OJPAEntityManager> instances = new ArrayList<OJPAEntityManager>();
  private final OJPAProperties properties;

  public OJPAEntityManagerFactory(final OJPAProperties properties) {
    this.properties = properties;
    if (logger.isLoggable(Level.INFO)) {
      logger.info("EntityManagerFactory created. " + toString());
    }
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public EntityManager createEntityManager(final Map map) {
    return createEntityManager(new OJPAProperties(map));
  }

  @Override
  public EntityManager createEntityManager() {
    return createEntityManager(properties);
  }

  private EntityManager createEntityManager(final OJPAProperties properties) {
    final OJPAEntityManager newInstance = new OJPAEntityManager(this, properties);
    instances.add(newInstance);
    return newInstance;
  }

  @Override
  public void close() {
    for (OJPAEntityManager instance : instances) {
      instance.close();
    }
    instances.clear();
    opened = false;
    if (logger.isLoggable(Level.INFO)) {
      logger.info("EntityManagerFactory closed. " + toString());
    }
  }

  @Override
  public boolean isOpen() {
    return opened;
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
  public Map<String, Object> getProperties() {
    return properties.getUnmodifiableProperties();
  }

  @Override
  public Cache getCache() {
    throw new UnsupportedOperationException("getCache");
  }

  @Override
  public PersistenceUnitUtil getPersistenceUnitUtil() {
    throw new UnsupportedOperationException("getPersistenceUnitUtil");
  }
}

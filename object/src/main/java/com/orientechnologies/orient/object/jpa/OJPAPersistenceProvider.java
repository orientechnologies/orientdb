/*
  *
  *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
  *  * For more information: http://www.orientechnologies.com
  *
  */
package com.orientechnologies.orient.object.jpa;

import com.orientechnologies.orient.core.entity.OEntityManager;
import com.orientechnologies.orient.object.jpa.parsing.PersistenceXmlUtil;

import javax.persistence.EntityManagerFactory;
import javax.persistence.spi.PersistenceProvider;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.persistence.spi.ProviderUtil;
import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.logging.Logger;

import static com.orientechnologies.orient.core.entity.OEntityManager.getEntityManagerByDatabaseURL;
import static com.orientechnologies.orient.object.jpa.parsing.PersistenceXmlUtil.PERSISTENCE_XML;

@SuppressWarnings("rawtypes")
public class OJPAPersistenceProvider implements PersistenceProvider {
  /** the log used by this class. */
  private static Logger           logger       = Logger.getLogger(OJPAPersistenceProvider.class.getName());
  private static OJPAProviderUtil providerUtil = new OJPAProviderUtil();

  private Collection<? extends PersistenceUnitInfo> persistenceUnits = null;

  public OJPAPersistenceProvider() {
    URL persistenceXml = Thread.currentThread().getContextClassLoader().getResource(PERSISTENCE_XML);
    if (persistenceXml != null)
      try {
        persistenceUnits = PersistenceXmlUtil.parse(persistenceXml);
      } catch (Exception e) {
        logger.info("Can't parse '" + PERSISTENCE_XML + "' :" + e.getMessage());
      }
  }

  @Override
  public synchronized EntityManagerFactory createEntityManagerFactory(String emName, Map map) {
    if (emName == null) {
      throw new IllegalStateException("Name of the persistence unit should not be null");
    }

    PersistenceUnitInfo unitInfo = PersistenceXmlUtil.findPersistenceUnit(emName, persistenceUnits);
    return createContainerEntityManagerFactory(unitInfo, map);
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized EntityManagerFactory createContainerEntityManagerFactory(PersistenceUnitInfo info, Map map) {

    OJPAProperties properties = ((info == null) ? new OJPAProperties() : (OJPAProperties) info.getProperties());

    // Override parsed properties with user specified
    if (map != null && !map.isEmpty()) {
      properties.putAll(map);
    }

    // register entities from <class> tag
    OEntityManager entityManager = getEntityManagerByDatabaseURL(properties.getURL());
    entityManager.registerEntityClasses(info.getManagedClassNames());

    return new OJPAEntityManagerFactory(properties);
  }

  @Override
  public ProviderUtil getProviderUtil() {
    return providerUtil;
  }
}

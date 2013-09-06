/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.object.jpa;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.logging.Logger;

import javax.persistence.EntityManagerFactory;
import javax.persistence.spi.PersistenceProvider;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.persistence.spi.ProviderUtil;

import com.orientechnologies.orient.object.jpa.parsing.PersistenceXmlUtil;

@SuppressWarnings("rawtypes")
public class OJPAPersistenceProvider implements PersistenceProvider {
	/** the log used by this class. */
	private static Logger															logger	= Logger.getLogger(PersistenceXmlUtil.class.getName());
	private Collection<? extends PersistenceUnitInfo>	persistenceUnits;

	public OJPAPersistenceProvider() {
		try {
			URL persistenceUnitRootUrl = new URL("file://" + PersistenceXmlUtil.PERSISTENCE_XML);
			persistenceUnits = PersistenceXmlUtil.parse(persistenceUnitRootUrl);
		} catch (MalformedURLException e) {
			logger.severe(e.getMessage());
		}
	}

	@Override
	public EntityManagerFactory createEntityManagerFactory(String emName, Map map) {
		if (emName == null) {
			throw new IllegalStateException("Name of the persistence unit should not be null");
		}

		synchronized (emName) {
			PersistenceUnitInfo unitInfo = PersistenceXmlUtil.findPersistenceUnit(emName, persistenceUnits);
			return createContainerEntityManagerFactory(unitInfo, map);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public EntityManagerFactory createContainerEntityManagerFactory(PersistenceUnitInfo info, Map map) {
		if (info == null || info.getPersistenceUnitName() == null) {
			throw new IllegalStateException("Name of the persistence unit should not be null");
		}

		synchronized (info) {
			if (map != null && !map.isEmpty()) {
				info.getProperties().putAll(map);
			}
			return new OJPAEntityManagerFactory((OJPAProperties) info.getProperties());
		}
	}

	@Override
	public ProviderUtil getProviderUtil() {
		throw new UnsupportedOperationException("getProviderUtil");
	}
}

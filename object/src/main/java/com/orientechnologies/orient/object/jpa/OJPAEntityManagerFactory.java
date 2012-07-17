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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Cache;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnitUtil;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.metamodel.Metamodel;

/**
 * JPA EntityManagerFactory implementation that uses OrientDB EntityManager instances. Can works also as singleton by using
 * OEntityManagerFactory.getInstance().
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OJPAEntityManagerFactory implements EntityManagerFactory {
	private boolean																opened		= true;
	private List<OJPAEntityManager>								instances	= new ArrayList<OJPAEntityManager>();
	private static final OJPAEntityManagerFactory	INSTANCE	= new OJPAEntityManagerFactory();

	public EntityManager createEntityManager() {
		return createEntityManager(new HashMap<Object, Object>());
	}

	@SuppressWarnings("rawtypes")
	public EntityManager createEntityManager(final Map map) {
		final OJPAEntityManager newInstance = new OJPAEntityManager(map);
		instances.add(newInstance);
		return newInstance;
	}

	public void close() {
		for (OJPAEntityManager instance : instances) {
			instance.close();
		}
		instances.clear();
		opened = false;
	}

	public boolean isOpen() {
		return opened;
	}

	public static OJPAEntityManagerFactory getInstance() {
		return INSTANCE;
	}

	public CriteriaBuilder getCriteriaBuilder() {
		throw new UnsupportedOperationException("getCriteriaBuilder");
	}

	public Metamodel getMetamodel() {
		throw new UnsupportedOperationException("getMetamodel");
	}

	public Map<String, Object> getProperties() {
		throw new UnsupportedOperationException("getProperties");
	}

	public Cache getCache() {
		throw new UnsupportedOperationException("getCache");
	}

	public PersistenceUnitUtil getPersistenceUnitUtil() {
		throw new UnsupportedOperationException("getPersistenceUnitUtil");
	}

}

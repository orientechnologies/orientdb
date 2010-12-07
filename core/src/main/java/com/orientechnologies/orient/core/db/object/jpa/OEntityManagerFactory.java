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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

/**
 * JPA EntityManagerFactory implementation that uses OrientDB EntityManager instances. Can works also as singleton by using
 * OEntityManagerFactory.getInstance().
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OEntityManagerFactory implements EntityManagerFactory {
	private boolean															opened		= true;
	private List<OEntityManager>								instances	= new ArrayList<OEntityManager>();
	private static final OEntityManagerFactory	INSTANCE	= new OEntityManagerFactory();

	public EntityManager createEntityManager() {
		return createEntityManager(null);
	}

	@SuppressWarnings("rawtypes")
	public EntityManager createEntityManager(Map map) {
		final OEntityManager newInstance = new OEntityManager(map);
		instances.add(newInstance);
		return newInstance;
	}

	public void close() {
		for (OEntityManager instance : instances) {
			instance.close();
		}
		instances.clear();
		opened = false;
	}

	public boolean isOpen() {
		return opened;
	}

	public static OEntityManagerFactory getInstance() {
		return INSTANCE;
	}
}

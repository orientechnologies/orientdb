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
package com.orientechnologies.orient.core.entity;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.OUser;

public class OEntityManager {
	private static final String		CLASS_EXTENSION	= ".class";
	private static final String		CLASS_SEPARATOR	= ".";

	private Map<String, Class<?>>	entityClasses		= new HashMap<String, Class<?>>();

	public OEntityManager() {
		registerEntityClass(OUser.class);
		registerEntityClass(ORole.class);
	}

	/**
	 * Create a POJO by its class name.
	 * 
	 * @see #registerEntityClasses(String)
	 */
	public Object createPojo(final String iClassName) throws OConfigurationException {
		if (iClassName == null)
			throw new IllegalArgumentException("Can't create the object: class name is empty");

		Class<?> entityClass = getEntityClass(iClassName);

		try {
			if (entityClass != null)
				return entityClass.newInstance();

		} catch (Exception e) {
			throw new OConfigurationException("Error while creating new pojo of class '" + iClassName + "'", e);
		}

		try {
			// TRY TO INSTANTIATE THE CLASS DIRECTLY BY ITS NAME
			return Class.forName(iClassName).newInstance();
		} catch (Exception e) {
			throw new OConfigurationException("The class '" + iClassName
					+ "' was not found between the entity classes. Assure to call the registerEntityClasses(package) before.");
		}
	}

	public Class<?> getEntityClass(final String iClassName) {
		return entityClasses.get(iClassName);
	}

	public void registerEntityClass(final Class<?> iClass) {
		entityClasses.put(iClass.getSimpleName(), iClass);
	}

	/**
	 * Scans all classes accessible from the context class loader which belong to the given package and subpackages.
	 * 
	 * @param iPackageName
	 *          The base package
	 * @return The classes
	 */
	public void registerEntityClasses(final String iPackageName) {
		OLogManager.instance().debug(this, "Discovering entity classes inside package: %s", iPackageName);

		try {
			final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
			assert classLoader != null;
			final String path = iPackageName.replace('.', '/');
			Enumeration<URL> resources;
			resources = classLoader.getResources(path);
			final List<File> dirs = new ArrayList<File>();
			while (resources.hasMoreElements()) {
				URL resource = resources.nextElement();
				dirs.add(new File(resource.getFile()));
			}
			for (File directory : dirs) {
				entityClasses.putAll(findClasses(directory, iPackageName));
			}

			if (OLogManager.instance().isDebugEnabled())
				for (Entry<String, Class<?>> entry : entityClasses.entrySet()) {
					OLogManager.instance().debug(this, "Loaded entity class '%s' from: %s", entry.getKey(), entry.getValue());
				}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Recursive method used to find all classes in a given directory and subdirs.
	 * 
	 * @param directory
	 *          The base directory
	 * @param packageName
	 *          The package name for classes found inside the base directory
	 * @return The classes
	 * @throws ClassNotFoundException
	 */
	private Map<String, Class<?>> findClasses(final File directory, final String packageName) throws ClassNotFoundException {
		final Map<String, Class<?>> classes = new HashMap<String, Class<?>>();
		if (!directory.exists())
			return classes;

		String className;
		final File[] files = directory.listFiles();
		for (File file : files) {
			if (file.isDirectory()) {
				if (file.getName().contains(CLASS_SEPARATOR))
					continue;
				classes.putAll(findClasses(file, packageName + CLASS_SEPARATOR + file.getName()));
			} else if (file.getName().endsWith(CLASS_EXTENSION)) {
				className = file.getName().substring(0, file.getName().length() - CLASS_EXTENSION.length());
				classes.put(className, Class.forName(packageName + '.' + className));
			}
		}
		return classes;
	}
}

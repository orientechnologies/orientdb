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
package com.orientechnologies.orient.core.db.object;

import java.util.List;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.ODatabaseSchemaAware;
import com.orientechnologies.orient.core.entity.OEntityManager;
import com.orientechnologies.orient.core.iterator.OObjectIteratorCluster;
import com.orientechnologies.orient.core.iterator.OObjectIteratorMultiCluster;

/**
 * Generic interface for object based Database implementations. Binds to/from Document and POJOs.
 * 
 * @author Luca Garulli
 */
public interface ODatabaseObject extends ODatabaseSchemaAware<Object> {

	/**
	 * Browses all the records of the specified cluster.
	 * 
	 * @param iClusterName
	 *          Cluster name to iterate
	 * @return Iterator of Object instances
	 */
	public <RET> OObjectIteratorCluster<RET> browseCluster(String iClusterName);

	/**
	 * Browses all the records of the specified class.
	 * 
	 * @param iClusterClass
	 *          Class name to iterate
	 * @return Iterator of Object instances
	 */
	public <RET> OObjectIteratorMultiCluster<RET> browseClass(Class<RET> iClusterClass);

	/**
	 * Execute a query against the database.
	 * 
	 * @param iCommand
	 *          Query command
	 * @return List of POJOs
	 */
	public <RET extends List<?>> RET query(final OCommandRequest iCommand);

	/**
	 * Creates a new entity of the specified class.
	 * 
	 * @param iType
	 *          Class name where to originate the instance
	 * @return New instance
	 */
	public <T> T newInstance(Class<T> iType);

	/**
	 * Returns the entity manager that handle the binding from ODocuments and POJOs.
	 * 
	 * @return
	 */
	public OEntityManager getEntityManager();

	/**
	 * Specifies if retain handled objects in memory or not. Setting it to false can improve performance on large inserts. Default is
	 * enabled.
	 * 
	 * @param iValue
	 *          True to enable, false to disable it.
	 */
	public void setRetainObjects(boolean iValue);
}

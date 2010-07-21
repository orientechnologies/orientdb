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

import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.record.ORecordInternal;

/**
 * Generic interface for record based Database implementations.
 * 
 * @author Luca Garulli
 */
public interface ODatabaseRecord<REC extends ORecordInternal<?>> extends ODatabaseComplex<REC> {

	/**
	 * Browses all the records of the specified cluster.
	 * 
	 * @param iClusterName
	 *          Cluster name to iterate
	 * @return Iterator of ODocument instances
	 */
	public ORecordIteratorCluster<REC> browseCluster(String iClusterName);

	/**
	 * Loads a record using a fetch plan.
	 * 
	 * @param iDocument
	 *          Record to load
	 * @param iFetchPlan
	 *          Fetch plan used
	 * @return The record received
	 */
	public REC load(REC iDocument, String iFetchPlan);

	/**
	 * Returns the record type class.
	 */
	public Class<? extends REC> getRecordType();

	/**
	 * Returns true if current configuration retains objects, otherwise false
	 * 
	 * @param iValue
	 *          True to enable, false to disable it.
	 * @see #setRetainRecords(boolean)
	 */
	public boolean isRetainRecords();

	/**
	 * Specifies if retain handled objects in memory or not. Setting it to false can improve performance on large inserts. Default is
	 * enabled.
	 * 
	 * @param iValue
	 *          True to enable, false to disable it.
	 * @see #isRetainRecords()
	 */
	public ODatabaseRecord<?> setRetainRecords(boolean iValue);

	/**
	 * Checks if the operation on a resource is allowed for the current user.
	 * 
	 * @param iResource
	 *          Resource where to execute the operation
	 * @param iOperation
	 *          Operation to execute against the resource
	 * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
	 */
	public <DB extends ODatabaseRecord<?>> DB checkSecurity(String iResource, int iOperation);

	/**
	 * Checks if the operation on a resource is allowed for the current user. The check is made in two steps:
	 * <ol>
	 * <li>
	 * Access to all the resource as *</li>
	 * <li>
	 * Access to the specific target resource</li>
	 * </ol>
	 * 
	 * @param iResourceGeneric
	 *          Resource where to execute the operation, i.e.: database.clusters
	 * @param iOperation
	 *          Operation to execute against the resource
	 * @param iResourcesTarget
	 *          Target resources as an array of Objects, i.e.: ["employee", 2] to specify cluster name and id.
	 * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
	 */
	public <DB extends ODatabaseRecord<?>> DB checkSecurity(String iResourceGeneric, int iOperation, Object... iResourcesSpecific);
}

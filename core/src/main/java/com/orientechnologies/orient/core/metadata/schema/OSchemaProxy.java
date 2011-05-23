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
package com.orientechnologies.orient.core.metadata.schema;

import java.util.Collection;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OProxedResource;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.OStorage.CLUSTER_TYPE;
import com.orientechnologies.orient.core.type.ODocumentWrapper;

/**
 * Proxy class to use the shared OSchemaShared instance. Before to delegate each operations it sets the current database in the
 * thread local.
 * 
 * @author Luca
 * 
 */
@SuppressWarnings("unchecked")
public class OSchemaProxy extends OProxedResource<OSchemaShared> implements OSchema {
	public OSchemaProxy(final OSchemaShared iDelegate, final ODatabaseRecord iDatabase) {
		super(iDelegate, iDatabase);
	}

	public void create() {
		setDatabaseInThreadLocal();
		delegate.create();
	}

	public int countClasses() {
		setDatabaseInThreadLocal();
		return delegate.countClasses();
	}

	public OClass createClass(final Class<?> iClass) {
		setDatabaseInThreadLocal();
		return delegate.createClass(iClass);
	}

	public String toString() {
		setDatabaseInThreadLocal();
		return delegate.toString();
	}

	public OClass createClass(final Class<?> iClass, final int iDefaultClusterId) {
		setDatabaseInThreadLocal();
		return delegate.createClass(iClass, iDefaultClusterId);
	}

	public OClass createClass(final String iClassName) {
		setDatabaseInThreadLocal();
		return delegate.createClass(iClassName);
	}

	public OClass createClass(final String iClassName, final OClass iSuperClass) {
		setDatabaseInThreadLocal();
		return delegate.createClass(iClassName, iSuperClass);
	}

	public OClass createClass(final String iClassName, final OClass iSuperClass, final CLUSTER_TYPE iType) {
		setDatabaseInThreadLocal();
		return delegate.createClass(iClassName, iSuperClass, iType);
	}

	public OClass createClass(final String iClassName, final int iDefaultClusterId) {
		setDatabaseInThreadLocal();
		return delegate.createClass(iClassName, iDefaultClusterId);
	}

	public OClass createClass(final String iClassName, final OClass iSuperClass, final int iDefaultClusterId) {
		setDatabaseInThreadLocal();
		return delegate.createClass(iClassName, iSuperClass, iDefaultClusterId);
	}

	public OClass createClass(final String iClassName, final OClass iSuperClass, final int[] iClusterIds) {
		setDatabaseInThreadLocal();
		return delegate.createClass(iClassName, iSuperClass, iClusterIds);
	}

	public OClass createClassInternal(final String iClassName, final OClass iSuperClass, final int[] iClusterIds) {
		setDatabaseInThreadLocal();
		return delegate.createClassInternal(iClassName, iSuperClass, iClusterIds);
	}

	public void dropClass(final String iClassName) {
		setDatabaseInThreadLocal();
		delegate.dropClass(iClassName);
	}

	public void dropClassInternal(final String iClassName) {
		setDatabaseInThreadLocal();
		delegate.dropClassInternal(iClassName);
	}

	public boolean existsClass(final String iClassName) {
		setDatabaseInThreadLocal();
		return delegate.existsClass(iClassName);
	}

	public OClass getClassById(final int iClassId) {
		setDatabaseInThreadLocal();
		return delegate.getClassById(iClassId);
	}

	public OClass getClass(final Class<?> iClass) {
		setDatabaseInThreadLocal();
		return delegate.getClass(iClass);
	}

	public OClass getClass(final String iClassName) {
		setDatabaseInThreadLocal();
		return delegate.getClass(iClassName);
	}

	public Collection<OClass> getClasses() {
		setDatabaseInThreadLocal();
		return delegate.getClasses();
	}

	public void load() {
		setDatabaseInThreadLocal();
		delegate.load();
	}

	public <RET extends ODocumentWrapper> RET reload() {
		setDatabaseInThreadLocal();
		return (RET) delegate.reload();
	}

	public <RET extends ODocumentWrapper> RET save() {
		setDatabaseInThreadLocal();
		return (RET) delegate.save();
	}

	public int getVersion() {
		setDatabaseInThreadLocal();
		return delegate.getVersion();
	}

	public void saveInternal() {
		setDatabaseInThreadLocal();
		delegate.saveInternal();
	}

	public ORID getIdentity() {
		return delegate.getIdentity();
	}
}

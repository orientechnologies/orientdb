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
		setCurrentDatabaseInThreadLocal();
		delegate.create();
	}

	public int countClasses() {
		setCurrentDatabaseInThreadLocal();
		return delegate.countClasses();
	}

	public OClass createClass(final Class<?> iClass) {
		setCurrentDatabaseInThreadLocal();
		return delegate.createClass(iClass);
	}

	public String toString() {
		return delegate.toString();
	}

	public OClass createClass(final Class<?> iClass, final int iDefaultClusterId) {
		setCurrentDatabaseInThreadLocal();
		return delegate.createClass(iClass, iDefaultClusterId);
	}

	public OClass createClass(final String iClassName) {
		setCurrentDatabaseInThreadLocal();
		return delegate.createClass(iClassName);
	}

	public OClass getOrCreateClass(final String iClassName) {
		setCurrentDatabaseInThreadLocal();
		return delegate.getOrCreateClass(iClassName);
	}

	public OClass createClass(final String iClassName, final OClass iSuperClass) {
		setCurrentDatabaseInThreadLocal();
		return delegate.createClass(iClassName, iSuperClass);
	}

	public OClass createClass(final String iClassName, final OClass iSuperClass, final CLUSTER_TYPE iType) {
		setCurrentDatabaseInThreadLocal();
		return delegate.createClass(iClassName, iSuperClass, iType);
	}

	public OClass createClass(final String iClassName, final int iDefaultClusterId) {
		setCurrentDatabaseInThreadLocal();
		return delegate.createClass(iClassName, iDefaultClusterId);
	}

	public OClass createClass(final String iClassName, final OClass iSuperClass, final int iDefaultClusterId) {
		setCurrentDatabaseInThreadLocal();
		return delegate.createClass(iClassName, iSuperClass, iDefaultClusterId);
	}

	public OClass createClass(final String iClassName, final OClass iSuperClass, final int[] iClusterIds) {
		setCurrentDatabaseInThreadLocal();
		return delegate.createClass(iClassName, iSuperClass, iClusterIds);
	}

	public OClass createClassInternal(final String iClassName, final OClass iSuperClass, final int[] iClusterIds) {
		setCurrentDatabaseInThreadLocal();
		return delegate.createClassInternal(iClassName, iSuperClass, iClusterIds);
	}

	public void dropClass(final String iClassName) {
		setCurrentDatabaseInThreadLocal();
		delegate.dropClass(iClassName);
	}

	public void dropClassInternal(final String iClassName) {
		setCurrentDatabaseInThreadLocal();
		delegate.dropClassInternal(iClassName);
	}

	public boolean existsClass(final String iClassName) {
		setCurrentDatabaseInThreadLocal();
		return delegate.existsClass(iClassName);
	}

	public OClass getClass(final Class<?> iClass) {
		setCurrentDatabaseInThreadLocal();
		return delegate.getClass(iClass);
	}

	public OClass getClass(final String iClassName) {
		setCurrentDatabaseInThreadLocal();
		return delegate.getClass(iClassName);
	}

	public Collection<OClass> getClasses() {
		setCurrentDatabaseInThreadLocal();
		return delegate.getClasses();
	}

	public void load() {
		setCurrentDatabaseInThreadLocal();
		delegate.load();
	}

	public <RET extends ODocumentWrapper> RET reload() {
		setCurrentDatabaseInThreadLocal();
		return (RET) delegate.reload();
	}

	public <RET extends ODocumentWrapper> RET save() {
		setCurrentDatabaseInThreadLocal();
		return (RET) delegate.save();
	}

	public int getVersion() {
		setCurrentDatabaseInThreadLocal();
		return delegate.getVersion();
	}

	public void saveInternal() {
		setCurrentDatabaseInThreadLocal();
		delegate.saveInternal();
	}

	public ORID getIdentity() {
		setCurrentDatabaseInThreadLocal();
		return delegate.getIdentity();
	}

	public void close() {
	}
}

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
package com.orientechnologies.orient.core.index;

import java.util.Collection;
import java.util.Set;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OProxedResource;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OIndexManagerProxy extends OProxedResource<OIndexManager> implements OIndexManager {

	public OIndexManagerProxy(final OIndexManager iDelegate, final ODatabaseRecord iDatabase) {
		super(iDelegate, iDatabase);
	}

	public OIndexManager load() {
		return this;
	}

	/**
	 * Force reloading of indexes.
	 */
	public OIndexManager reload() {
		return delegate.load();
	}

	public void create() {
		delegate.create();
	}

	public Collection<? extends OIndex<?>> getIndexes() {
		return delegate.getIndexes();
	}

	public OIndex<?> getIndex(final String iName) {
		return delegate.getIndex(iName);
	}

	public OIndex<?> getIndex(final ORID iRID) {
		return delegate.getIndex(iRID);
	}

	public OIndex<?> createIndex(final String iName, final String iType, final OIndexDefinition iIndexDefinition,
			final int[] iClusterIdsToIndex, final OProgressListener iProgressListener) {
		return delegate.createIndex(iName, iType, iIndexDefinition, iClusterIdsToIndex, iProgressListener);
	}

	public OIndex<?> getIndexInternal(final String iName) {
		return ((OIndexManagerShared) delegate).getIndexInternal(iName);
	}

	public ODocument getConfiguration() {
		return delegate.getConfiguration();
	}

	public OIndexManager dropIndex(final String iIndexName) {
		return delegate.dropIndex(iIndexName);
	}

	public String getDefaultClusterName() {
		return delegate.getDefaultClusterName();
	}

	public void setDefaultClusterName(final String defaultClusterName) {
		delegate.setDefaultClusterName(defaultClusterName);
	}

	public ODictionary<ORecordInternal<?>> getDictionary() {
		return delegate.getDictionary();
	}

	public void flush() {
		delegate.flush();
	}

	public Set<OIndex<?>> getClassInvolvedIndexes(final String className, final Collection<String> fields) {
		return delegate.getClassInvolvedIndexes(className, fields);
	}

	public Set<OIndex<?>> getClassInvolvedIndexes(final String className, final String... fields) {
		return delegate.getClassInvolvedIndexes(className, fields);
	}

	public boolean areIndexed(final String className, final Collection<String> fields) {
		return delegate.areIndexed(className, fields);
	}

	public boolean areIndexed(final String className, final String... fields) {
		return delegate.areIndexed(className, fields);
	}

	public Set<OIndex<?>> getClassIndexes(final String className) {
		return delegate.getClassIndexes(className);
	}

	public OIndex<?> getClassIndex(final String className, final String indexName) {
		return delegate.getClassIndex(className, indexName);
	}
}

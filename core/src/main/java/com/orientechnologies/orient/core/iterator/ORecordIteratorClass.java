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
package com.orientechnologies.orient.core.iterator;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Iterator class to browse forward and backward the records of a cluster. Once browsed in a direction, the iterator cannot change
 * it. This iterator with "live updates" set is able to catch updates to the cluster sizes while browsing. This is the case when
 * concurrent clients/threads insert and remove item in any cluster the iterator is browsing. If the cluster are hot removed by from
 * the database the iterator could be invalid and throw exception of cluster not found.
 * 
 * @author Luca Garulli
 * 
 * @param <T>
 *          Record Type
 */
public class ORecordIteratorClass<REC extends ORecordInternal<?>> extends ORecordIteratorClusters<REC> {
	protected final OClass	targetClass;
	protected boolean				polymorphic;

	public ORecordIteratorClass(final ODatabaseRecord iDatabase, final ODatabaseRecordAbstract iLowLevelDatabase,
			final String iClassName, final boolean iPolymorphic) {
		super(iDatabase, iLowLevelDatabase);

		targetClass = database.getMetadata().getSchema().getClass(iClassName);
		if (targetClass == null)
			throw new IllegalArgumentException("Class '" + iClassName + "' was not found in database schema");

		polymorphic = iPolymorphic;
		clusterIds = polymorphic ? targetClass.getPolymorphicClusterIds() : targetClass.getClusterIds();

		config();
	}

	@SuppressWarnings("unchecked")
	@Override
	public REC next() {
		return (REC) super.next().getRecord();
	}

	@SuppressWarnings("unchecked")
	@Override
	public REC previous() {
		return (REC) super.previous().getRecord();
	}

	@Override
	protected boolean include(final ORecord<?> record) {
		return record instanceof ODocument && targetClass.isSuperClassOf(((ODocument) record).getSchemaClass());
	}

	public boolean isPolymorphic() {
		return polymorphic;
	}
}

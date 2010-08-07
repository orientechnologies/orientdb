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
package com.orientechnologies.orient.core.type.tree;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;

/**
 * Save changes on express call. Useful for massive changes.
 * 
 * @author Luca Garulli
 */
@SuppressWarnings("serial")
public class OTreeMapDatabaseLazySave<K, V> extends OTreeMapDatabase<K, V> {

	public OTreeMapDatabaseLazySave(ODatabaseRecord<?> iDatabase, ORID iRID) {
		super(iDatabase, iRID);
	}

	public OTreeMapDatabaseLazySave(ODatabaseRecord<?> iDatabase, String iClusterName, OStreamSerializer iKeySerializer,
			OStreamSerializer iValueSerializer) {
		super(iDatabase, iClusterName, iKeySerializer, iValueSerializer);
	}

	@Override
	public void commitChanges(final ODatabaseRecord<?> iDatabase) {
	}

	public void lazySave() {
		super.commitChanges(database);
	}
}

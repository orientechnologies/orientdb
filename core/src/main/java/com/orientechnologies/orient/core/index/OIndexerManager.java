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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.hook.ORecordHookAbstract;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.tree.OTreeMapDatabase;

public class OIndexerManager extends ORecordHookAbstract {

	@Override
	public void onRecordAfterCreate(final ORecord<?> iRecord) {
		Map<OProperty, String> indexedProperties = getIndexedProperties(iRecord);

		if (indexedProperties != null)
			for (Entry<OProperty, String> prop : indexedProperties.entrySet()) {
				prop.getKey().getIndex().put(prop.getValue(), (ODocument) iRecord);
			}
	}

	@Override
	public void onRecordAfterUpdate(final ORecord<?> iRecord) {

	}

	@Override
	public void onRecordAfterDelete(final ORecord<?> iRecord) {

	}

	protected Map<OProperty, String> getIndexedProperties(final ORecord<?> iRecord) {
		if (!(iRecord instanceof ORecordSchemaAware<?>))
			return null;

		final ORecordSchemaAware<?> record = (ORecordSchemaAware<?>) iRecord;
		final OClass cls = record.getSchemaClass();
		if (cls == null)
			return null;

		OTreeMapDatabase<String, ODocument> index;
		Object fieldValue;
		String fieldValueString;

		final Map<OProperty, String> indexedProperties = new HashMap<OProperty, String>();

		for (OProperty prop : cls.properties()) {
			index = prop.getIndex();
			if (index != null) {
				fieldValue = record.field(prop.getName());

				if (fieldValue != null) {
					fieldValueString = fieldValue.toString();
					if (index.containsKey(fieldValueString))
						OLogManager.instance().exception("Found duplicated key '%s' for property '%s'", null, OIndexException.class,
								fieldValueString, prop);

					indexedProperties.put(prop, fieldValueString);
				}
			}
		}

		return indexedProperties;
	}
}

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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Handles indexing when records change.
 * 
 * @author Luca Garulli
 * 
 */
public class OIndexerManager extends ODocumentHookAbstract {

	@Override
	public void onRecordBeforeCreate(final ODocument iRecord) {
		checkIndexedProperties(iRecord);
	}

	@Override
	public void onRecordAfterCreate(final ODocument iRecord) {
		final Map<OProperty, String> indexedProperties = getIndexedProperties(iRecord);

		if (indexedProperties != null)
			for (Entry<OProperty, String> propEntry : indexedProperties.entrySet()) {
				propEntry.getKey().getIndex().put(propEntry.getValue(), (ORecordId) iRecord.getIdentity());
				propEntry.getKey().getIndex().lazySave();
			}
	}

	@Override
	public void onRecordBeforeUpdate(final ODocument iRecord) {
		checkIndexedProperties(iRecord);
	}

	@Override
	public void onRecordAfterUpdate(final ODocument iRecord) {
		final Map<OProperty, String> indexedProperties = getIndexedProperties(iRecord);

		if (indexedProperties != null) {
			final Set<String> dirtyFields = iRecord.getDirtyFields();

			if (dirtyFields != null && dirtyFields.size() > 0) {
				// REMOVE INDEX OF ENTRIES FOR THE OLD VALUES
				for (Entry<OProperty, String> propEntry : indexedProperties.entrySet()) {
					if (dirtyFields.contains(propEntry.getKey().getName())) {
						// REMOVE IT
						propEntry.getKey().getIndex().remove(iRecord.getOriginalValue(propEntry.getKey().getName()));
						propEntry.getKey().getIndex().lazySave();
					}
				}

				// ADD INDEX OF ENTRIES FOR THE CHANGED ONLY VALUES
				for (Entry<OProperty, String> propEntry : indexedProperties.entrySet()) {
					if (dirtyFields.contains(propEntry.getKey().getName())) {
						propEntry.getKey().getIndex().put(propEntry.getValue(), (ORecordId) iRecord.getIdentity());
						propEntry.getKey().getIndex().lazySave();
					}
				}
			}
		}
	}

	@Override
	public void onRecordAfterDelete(final ODocument iRecord) {
		final Map<OProperty, String> indexedProperties = getIndexedProperties(iRecord);

		if (indexedProperties != null) {
			final Set<String> dirtyFields = iRecord.getDirtyFields();

			if (dirtyFields != null && dirtyFields.size() > 0) {
				// REMOVE INDEX OF ENTRIES FOR THE OLD VALUES
				for (Entry<OProperty, String> propEntry : indexedProperties.entrySet()) {
					if (dirtyFields.contains(propEntry.getKey().getName())) {
						// REMOVE IT
						propEntry.getKey().getIndex().remove(propEntry.getValue());
						propEntry.getKey().getIndex().lazySave();
					}
				}
			}

			// REMOVE INDEX OF ENTRIES FOR THE CHANGED ONLY VALUES
			for (Entry<OProperty, String> propEntry : indexedProperties.entrySet()) {
				if (iRecord.containsField(propEntry.getKey().getName())
						&& (dirtyFields == null || !dirtyFields.contains(propEntry.getKey().getName()))) {
					propEntry.getKey().getIndex().remove(propEntry.getValue());
					propEntry.getKey().getIndex().lazySave();
				}
			}
		}
	}

	protected void checkIndexedProperties(final ODocument iRecord) {
		final OClass cls = iRecord.getSchemaClass();
		if (cls == null)
			return;

		OIndex index;
		Object fieldValue;
		String fieldValueString;

		List<ORecordId> indexedRIDs;

		for (OProperty prop : cls.properties()) {
			index = prop.getIndex();
			if (index != null && index.isUnique()) {
				fieldValue = iRecord.field(prop.getName());

				if (fieldValue != null) {
					fieldValueString = fieldValue.toString();

					indexedRIDs = index.get(fieldValueString);
					if (indexedRIDs != null && indexedRIDs.size() > 0 && !indexedRIDs.get(0).equals(iRecord.getIdentity()))
						OLogManager.instance().exception("Found duplicated key '%s' for property '%s'", null, OIndexException.class,
								fieldValueString, prop);
				}
			}
		}
	}

	protected Map<OProperty, String> getIndexedProperties(final ODocument iRecord) {
		final ORecordSchemaAware<?> record = (ORecordSchemaAware<?>) iRecord;
		final OClass cls = record.getSchemaClass();
		if (cls == null)
			return null;

		OIndex index;
		Object fieldValue;
		String fieldValueString;

		Map<OProperty, String> indexedProperties = null;

		for (OProperty prop : cls.properties()) {
			index = prop.getIndex();
			if (index != null) {
				fieldValue = record.field(prop.getName());

				if (fieldValue != null) {
					fieldValueString = fieldValue.toString();

					// PUSH THE PROPERTY IN THE SET TO BE WORKED BY THE EXTERNAL
					if (indexedProperties == null)
						indexedProperties = new HashMap<OProperty, String>();
					indexedProperties.put(prop, fieldValueString);
				}
			}
		}

		return indexedProperties;
	}
}

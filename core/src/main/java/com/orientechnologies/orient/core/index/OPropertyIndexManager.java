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
import java.util.Set;

import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Handles indexing when records change.
 * 
 * @author Luca Garulli
 * 
 */
public class OPropertyIndexManager extends ODocumentHookAbstract {

	@Override
	public boolean onRecordBeforeCreate(final ODocument iRecord) {
		checkIndexedProperties(iRecord);
		return false;
	}

	@Override
	public boolean onRecordAfterCreate(final ODocument iRecord) {
		final Map<OProperty, String> indexedProperties = getIndexedProperties(iRecord);

		if (indexedProperties != null)
			for (Entry<OProperty, String> propEntry : indexedProperties.entrySet()) {
				propEntry.getKey().getIndex().getUnderlying().put(propEntry.getValue(), (ORecordId) iRecord.getIdentity());
			}
		return false;
	}

	@Override
	public boolean onRecordBeforeUpdate(final ODocument iRecord) {
		checkIndexedProperties(iRecord);
		return false;
	}

	@Override
	public boolean onRecordAfterUpdate(final ODocument iRecord) {
		final Map<OProperty, String> indexedProperties = getIndexedProperties(iRecord);

		if (indexedProperties != null) {
			final Set<String> dirtyFields = iRecord.getDirtyFields();

			if (dirtyFields != null && dirtyFields.size() > 0) {
				// REMOVE INDEX OF ENTRIES FOR THE OLD VALUES
				Object originalValue = null;
				OIndex index;

				for (Entry<OProperty, String> propEntry : indexedProperties.entrySet()) {
					if (dirtyFields.contains(propEntry.getKey().getName())) {
						// REMOVE IT
						originalValue = iRecord.getOriginalValue(propEntry.getKey().getName());
						if (originalValue != null)
							originalValue = originalValue.toString();

						index = propEntry.getKey().getIndex().getUnderlying();

						index.remove(originalValue);
						index.lazySave();
					}
				}

				// ADD INDEX OF ENTRIES FOR THE CHANGED ONLY VALUES
				for (Entry<OProperty, String> propEntry : indexedProperties.entrySet()) {
					if (dirtyFields.contains(propEntry.getKey().getName())) {
						index = propEntry.getKey().getIndex().getUnderlying();

						index.put(propEntry.getValue(), (ORecordId) iRecord.getIdentity());
						index.lazySave();
					}
				}
			}
		}
		return false;
	}

	@Override
	public boolean onRecordAfterDelete(final ODocument iRecord) {
		final Map<OProperty, String> indexedProperties = getIndexedProperties(iRecord);

		if (indexedProperties != null) {
			final Set<String> dirtyFields = iRecord.getDirtyFields();

			OIndex index;

			if (dirtyFields != null && dirtyFields.size() > 0) {
				// REMOVE INDEX OF ENTRIES FOR THE OLD VALUES
				for (Entry<OProperty, String> propEntry : indexedProperties.entrySet()) {
					if (dirtyFields.contains(propEntry.getKey().getName())) {
						// REMOVE IT
						index = propEntry.getKey().getIndex().getUnderlying();
						index.remove(propEntry.getValue());
						index.lazySave();
					}
				}
			}

			// REMOVE INDEX OF ENTRIES FOR THE CHANGED ONLY VALUES
			for (Entry<OProperty, String> propEntry : indexedProperties.entrySet()) {
				if (iRecord.containsField(propEntry.getKey().getName())
						&& (dirtyFields == null || !dirtyFields.contains(propEntry.getKey().getName()))) {
					index = propEntry.getKey().getIndex().getUnderlying();
					index.remove(propEntry.getValue());
					index.lazySave();
				}
			}
		}
		return false;
	}

	protected void checkIndexedProperties(final ODocument iRecord) {
		final OClass cls = iRecord.getSchemaClass();
		if (cls == null)
			return;

		OPropertyIndex index;
		for (OProperty prop : cls.properties()) {
			index = prop.getIndex();
			if (index != null)
				index.checkEntry(iRecord);
		}
	}

	protected Map<OProperty, String> getIndexedProperties(final ODocument iRecord) {
		final ORecordSchemaAware<?> record = iRecord;
		final OClass cls = record.getSchemaClass();
		if (cls == null)
			return null;

		OPropertyIndex index;
		Object fieldValue;
		String fieldValueString;

		Map<OProperty, String> indexedProperties = null;

		for (OProperty prop : cls.properties()) {
			index = prop.getIndex();
			if (index != null) {
				if (prop.getType() == OType.LINK)
					// GET THE RID TO AVOID LOADING
					fieldValue = record.field(prop.getName(), OType.LINK);
				else
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

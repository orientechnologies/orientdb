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

import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
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
		final Map<OProperty, Object> indexedProperties = getIndexedProperties(iRecord);

		if (indexedProperties != null)
			for (Entry<OProperty, Object> propEntry : indexedProperties.entrySet()) {
				// SAVE A COPY TO AVOID PROBLEM ON RECYCLING OF THE RECORD
				propEntry.getKey().getIndex().getUnderlying().put(propEntry.getValue(), iRecord.placeholder());
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
		final Map<OProperty, Object> indexedProperties = getIndexedProperties(iRecord);

		if (indexedProperties != null) {
			final String[] dirtyFields = iRecord.getDirtyFields();

			if (dirtyFields.length > 0) {
				// REMOVE INDEX OF ENTRIES FOR THE OLD VALUES
				Object originalValue = null;
				OIndex<?> index;

				for (Entry<OProperty, Object> propEntry : indexedProperties.entrySet()) {
					for (String f : dirtyFields)
						if (f.equals(propEntry.getKey().getName())) {
							// REMOVE IT
							originalValue = iRecord.getOriginalValue(propEntry.getKey().getName());

							index = propEntry.getKey().getIndex().getUnderlying();

							index.remove(originalValue, iRecord);

							break;
						}
				}

				// ADD INDEX OF ENTRIES FOR THE CHANGED ONLY VALUES
				for (Entry<OProperty, Object> propEntry : indexedProperties.entrySet()) {
					for (String f : dirtyFields)
						if (f.equals(propEntry.getKey().getName())) {
							index = propEntry.getKey().getIndex().getUnderlying();

							// SAVE A COPY TO AVOID PROBLEM ON RECYCLING OF THE RECORD
							index.put(propEntry.getValue(), iRecord.placeholder());

							break;
						}
				}
			}
		}

		if (iRecord.isTrackingChanges()) {
			iRecord.setTrackingChanges(false);
			iRecord.setTrackingChanges(true);
		}

		return false;
	}

	@Override
	public boolean onRecordAfterDelete(final ODocument iRecord) {
		final Map<OProperty, Object> indexedProperties = getIndexedProperties(iRecord);

		if (indexedProperties != null) {
			final String[] dirtyFields = iRecord.getDirtyFields();

			OIndex<?> index;

			if (dirtyFields.length > 0) {
				// REMOVE INDEX OF ENTRIES FOR THE OLD VALUES
				for (Entry<OProperty, Object> propEntry : indexedProperties.entrySet()) {
					for (String f : dirtyFields)
						if (f.equals(propEntry.getKey().getName())) {
							// REMOVE IT
							index = propEntry.getKey().getIndex().getUnderlying();
							index.remove(iRecord.getOriginalValue(propEntry.getKey().getName()), iRecord);
							break;
						}
				}
			}

			// REMOVE INDEX OF ENTRIES FOR THE CHANGED ONLY VALUES
			for (Entry<OProperty, Object> propEntry : indexedProperties.entrySet()) {
				if (iRecord.containsField(propEntry.getKey().getName())) {
					boolean found = false;
					for (String f : dirtyFields)
						if (f.equals(propEntry.getKey().getName())) {
							found = true;
							break;
						}

					if (!found) {
						index = propEntry.getKey().getIndex().getUnderlying();
						index.remove(propEntry.getValue(), iRecord);
					}
				}
			}
		}

		if (iRecord.isTrackingChanges()) {
			iRecord.setTrackingChanges(false);
			iRecord.setTrackingChanges(true);
		}

		return false;
	}

	protected void checkIndexedProperties(final ODocument iRecord) {
		final OClass cls = iRecord.getSchemaClass();
		if (cls == null)
			return;

		OPropertyIndex index;
		for (OProperty prop : cls.getIndexedProperties()) {
			index = prop.getIndex();
			if (index != null)
				index.checkEntry(iRecord);
		}
	}

	protected Map<OProperty, Object> getIndexedProperties(final ODocument iRecord) {
		final ORecordSchemaAware<?> record = iRecord;
		final OClass cls = record.getSchemaClass();
		if (cls == null)
			return null;

		OPropertyIndex index;
		Object fieldValue;

		Map<OProperty, Object> indexedProperties = null;

		for (OProperty prop : cls.getIndexedProperties()) {
			index = prop.getIndex();
			if (index != null) {
				if (prop.getType() == OType.LINK)
					// GET THE RID TO AVOID LOADING
					fieldValue = record.field(prop.getName(), OType.LINK);
				else
					fieldValue = record.field(prop.getName());

				if (fieldValue != null) {
					// PUSH THE PROPERTY IN THE SET TO BE WORKED BY THE EXTERNAL
					if (indexedProperties == null)
						indexedProperties = new HashMap<OProperty, Object>();
					indexedProperties.put(prop, fieldValue);
				}
			}
		}

		return indexedProperties;
	}
}

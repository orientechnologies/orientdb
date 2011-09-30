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

import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.*;
import java.util.Map.Entry;

/**
 * Handles indexing when records change.
 *
 * @author Luca Garulli
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

		if (indexedProperties != null) {
			for (Entry<OProperty, Object> propEntry : indexedProperties.entrySet()) {
				// SAVE A COPY TO AVOID PROBLEM ON RECYCLING OF THE RECORD
				Object value = propEntry.getValue();
				OIndex index = propEntry.getKey().getIndex().getUnderlying();
				if (isCollection(value)) {
					for (Object item : toCollection(value)) {
						index.put(item, iRecord.placeholder());
					}
				} else {
					index.put(value, iRecord.placeholder());
				}
			}
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
				Object originalValue;
				OIndex<?> index;

				for (Entry<OProperty, Object> propEntry : indexedProperties.entrySet()) {
					for (String f : dirtyFields) {
						if (f.equals(propEntry.getKey().getName())) {
							originalValue = iRecord.getOriginalValue(propEntry.getKey().getName());
							Object newValue = propEntry.getValue();
							index = propEntry.getKey().getIndex().getUnderlying();

							if (isCollection(originalValue) || isCollection(newValue)) {
								updateCollectionIndex(index, originalValue, newValue, iRecord);
							} else {
								// REMOVE IT
								index.remove(originalValue, iRecord);
								// SAVE A COPY TO AVOID PROBLEM ON RECYCLING OF THE RECORD
								index.put(propEntry.getValue(), iRecord.placeholder());
							}
							break;
						}
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
					for (String f : dirtyFields) {
						if (f.equals(propEntry.getKey().getName())) {
							// REMOVE IT
							Object originalValue = iRecord.getOriginalValue(propEntry.getKey().getName());
							index = propEntry.getKey().getIndex().getUnderlying();
							if (isCollection(originalValue)) {
								for (Object item : toCollection(originalValue)) {
									index.remove(item, iRecord);
								}
							} else {
								index.remove(originalValue, iRecord);
							}
							break;
						}
					}
				}
			}

			// REMOVE INDEX OF ENTRIES FOR THE CHANGED ONLY VALUES
			for (Entry<OProperty, Object> propEntry : indexedProperties.entrySet()) {
				if (iRecord.containsField(propEntry.getKey().getName())) {
					boolean found = false;
					for (String f : dirtyFields) {
						if (f.equals(propEntry.getKey().getName())) {
							found = true;
							break;
						}
					}

					if (!found) {
						Object value = propEntry.getValue();
						index = propEntry.getKey().getIndex().getUnderlying();
						if (isCollection(value)) {
							for (Object item : toCollection(value)) {
								index.remove(item, iRecord);
							}
						} else {
							index.remove(value, iRecord);
						}
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

	private void updateCollectionIndex(OIndex index, Object old, Object with, ODocument iRecord) {
		Collection<?> oldCollection = toCollection(old);
		// Convert the new collection to a new hashset, so we can efficiently check if keys from the old collection are
		// are found in it, and if so remove them so we're left with only the new keys to add
		Collection<?> newCollection = new HashSet(toCollection(with));
		for (Object oldItem : oldCollection) {
			if (!newCollection.contains(oldItem)) {
				index.remove(oldItem, iRecord);
			} else {
				newCollection.remove(oldItem);
			}
		}
		// Index the remaining
		for (Object newItem : newCollection) {
			index.put(newItem, iRecord.placeholder());
		}
	}

	private Collection<?> toCollection(Object o) {
		if (o instanceof Collection) {
			return (Collection) o;
		} else if (o == null) {
			return Collections.emptySet();
		} else {
			return Collections.singleton(o);
		}
	}

	private boolean isCollection(Object o) {
		return o instanceof Collection;
	}
}

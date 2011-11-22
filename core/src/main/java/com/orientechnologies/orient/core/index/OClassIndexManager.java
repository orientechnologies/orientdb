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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Handles indexing when records change.
 * 
 * @author Andrey Lomakin, Artem Orobets
 */
public class OClassIndexManager extends ODocumentHookAbstract {
	@Override
	public boolean onRecordBeforeCreate(ODocument iRecord) {
		iRecord = checkForLoading(iRecord);

		checkIndexedProperties(iRecord);
		return false;
	}

	@Override
	public void onRecordAfterCreate(ODocument iRecord) {
		iRecord = checkForLoading(iRecord);

		final OClass cls = iRecord.getSchemaClass();
		if (cls != null) {
			final Collection<OIndex<?>> indexes = cls.getIndexes();
			for (final OIndex<?> index : indexes) {
				final Object key = index.getDefinition().getDocumentValueToIndex(iRecord);
				// SAVE A COPY TO AVOID PROBLEM ON RECYCLING OF THE RECORD
				if (key instanceof Collection) {
					for (final Object keyItem : (Collection<?>) key)
						if (keyItem != null)
							index.put(keyItem, iRecord.placeholder());
				} else if (key != null)
					index.put(key, iRecord.placeholder());
			}
		}
	}

	@Override
	public boolean onRecordBeforeUpdate(ODocument iRecord) {
		iRecord = checkForLoading(iRecord);

		checkIndexedProperties(iRecord);
		return false;
	}

	@Override
	public void onRecordAfterUpdate(ODocument iRecord) {
		iRecord = checkForLoading(iRecord);

		final OClass cls = iRecord.getSchemaClass();
		if (cls == null)
			return;

		final Collection<OIndex<?>> indexes = cls.getIndexes();

		if (!indexes.isEmpty()) {
			final Set<String> dirtyFields = new HashSet<String>(Arrays.asList(iRecord.getDirtyFields()));

			if (!dirtyFields.isEmpty()) {
				for (final OIndex<?> index : indexes) {
					final OIndexDefinition indexDefinition = index.getDefinition();
					final List<String> indexFields = indexDefinition.getFields();

					for (final String indexField : indexFields) {
						if (dirtyFields.contains(indexField)) {
							final List<Object> origValues = new ArrayList<Object>(indexFields.size());

							for (final String field : indexFields) {
								if (dirtyFields.contains(field))
									origValues.add(iRecord.getOriginalValue(field));
								else
									origValues.add(iRecord.<Object> field(field));
							}

							final Object origValue = indexDefinition.createValue(origValues);
							final Object newValue = indexDefinition.getDocumentValueToIndex(iRecord);

							if ((origValue instanceof Collection) && (newValue instanceof Collection)) {
								final Set<Object> valuesToRemove = new HashSet<Object>((Collection<?>) origValue);
								final Set<Object> valuesToAdd = new HashSet<Object>((Collection<?>) newValue);

								valuesToRemove.removeAll((Collection<?>) newValue);
								valuesToAdd.removeAll((Collection<?>) origValue);

								for (final Object valueToRemove : valuesToRemove)
									if (valueToRemove != null)
										index.remove(valueToRemove, iRecord);

								for (final Object valueToAdd : valuesToAdd)
									if (valueToAdd != null)
										index.put(valueToAdd, iRecord);

							} else {
								if (origValue instanceof Collection) {
									for (final Object origValueItem : (Collection<?>) origValue)
										if (origValueItem != null)
											index.remove(origValueItem, iRecord);
								} else if (origValue != null)
									index.remove(origValue, iRecord);

								if (newValue instanceof Collection) {
									for (final Object newValueItem : (Collection<?>) newValue)
										index.put(newValueItem, iRecord.placeholder());
								} else if (newValue != null)
									index.put(newValue, iRecord.placeholder());
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
	}

	@Override
	public boolean onRecordBeforeDelete(final ODocument iDocument) {
		if (iDocument.fields() == 0)
			// FORCE LOADING OF CLASS+FIELDS TO USE IT AFTER ON onRecordAfterDelete METHOD
			iDocument.reload();
		return false;
	};

	@Override
	public void onRecordAfterDelete(final ODocument iRecord) {
		final OClass cls = iRecord.getSchemaClass();
		if (cls == null)
			return;

		final Collection<OIndex<?>> indexes = new ArrayList<OIndex<?>>(cls.getIndexes());

		if (!indexes.isEmpty()) {
			final Set<String> dirtyFields = new HashSet<String>(Arrays.asList(iRecord.getDirtyFields()));

			if (!dirtyFields.isEmpty()) {
				// REMOVE INDEX OF ENTRIES FOR THE OLD VALUES
				final Iterator<OIndex<?>> indexIterator = indexes.iterator();

				while (indexIterator.hasNext()) {
					final OIndex<?> index = indexIterator.next();
					final OIndexDefinition indexDefinition = index.getDefinition();

					final List<String> indexFields = indexDefinition.getFields();
					for (final String indexField : indexFields) {
						// REMOVE IT
						if (dirtyFields.contains(indexField)) {
							final List<Object> origValues = new ArrayList<Object>(indexFields.size());

							for (final String field : indexFields) {
								if (dirtyFields.contains(field))
									origValues.add(iRecord.getOriginalValue(field));
								else
									origValues.add(iRecord.<Object> field(field));
							}

							final Object origValue = indexDefinition.createValue(origValues);
							if (origValue instanceof Collection) {
								for (final Object valueItem : (Collection<?>) origValue)
									if (valueItem != null)
										index.remove(valueItem, iRecord);
							} else if (origValue != null)
								index.remove(origValue, iRecord);

							indexIterator.remove();
							break;
						}
					}
				}
			}

			// REMOVE INDEX OF ENTRIES FOR THE NON CHANGED ONLY VALUES
			for (final OIndex<?> index : indexes) {
				final Object key = index.getDefinition().getDocumentValueToIndex(iRecord);
				if (key instanceof Collection) {
					for (final Object keyItem : (Collection<?>) key)
						if (keyItem != null)
							index.remove(keyItem, iRecord);
				} else if (key != null)
					index.remove(key, iRecord);
			}
		}

		if (iRecord.isTrackingChanges()) {
			iRecord.setTrackingChanges(false);
			iRecord.setTrackingChanges(true);
		}
	}

	private void checkIndexedProperties(final ODocument iRecord) {
		final OClass cls = iRecord.getSchemaClass();
		if (cls == null)
			return;

		final Collection<OIndex<?>> indexes = cls.getIndexes();
		for (final OIndex<?> index : indexes) {
			final Object key = index.getDefinition().getDocumentValueToIndex(iRecord);
			if (key instanceof Collection) {
				for (final Object keyItem : (Collection<?>) key)
					index.getInternal().checkEntry(iRecord, keyItem);
			} else
				index.getInternal().checkEntry(iRecord, key);
		}

	}

	private ODocument checkForLoading(final ODocument iRecord) {
		if (iRecord.getInternalStatus() == ORecordElement.STATUS.NOT_LOADED) {
			try {
				return (ODocument) iRecord.load();
			} catch (final ORecordNotFoundException e) {
				throw new OIndexException("Error during loading of record with id : " + iRecord.getIdentity());
			}
		}
		return iRecord;
	}
}

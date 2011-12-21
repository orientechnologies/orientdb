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

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.orientechnologies.common.collection.OCompositeKey;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.ODocumentWrapperNoClass;

/**
 * Index that consist of several indexDefinitions like {@link OPropertyIndexDefinition}.
 */

public class OCompositeIndexDefinition extends ODocumentWrapperNoClass implements OIndexDefinition {
	private final List<OIndexDefinition>	indexDefinitions;
	private String												className;

	public OCompositeIndexDefinition() {
		indexDefinitions = new LinkedList<OIndexDefinition>();
	}

	/**
	 * Constructor for new index creation.
	 * 
	 * @param iClassName
	 *          - name of class which is owner of this index
	 */
	public OCompositeIndexDefinition(final String iClassName) {
		super(new ODocument());

		indexDefinitions = new LinkedList<OIndexDefinition>();
		className = iClassName;
	}

	/**
	 * Constructor for new index creation.
	 * 
	 * @param iClassName
	 *          - name of class which is owner of this index
	 * @param iIndexes
	 *          List of indexDefinitions to add in given index.
	 */
	public OCompositeIndexDefinition(final String iClassName, final List<? extends OIndexDefinition> iIndexes) {
		super(new ODocument());
		indexDefinitions = new LinkedList<OIndexDefinition>();
		indexDefinitions.addAll(iIndexes);
		className = iClassName;
	}

	/**
	 * {@inheritDoc}
	 */
	public String getClassName() {
		return className;
	}

	/**
	 * Add new indexDefinition in current composite.
	 * 
	 * @param indexDefinition
	 *          Index to add.
	 */
	public void addIndex(final OIndexDefinition indexDefinition) {
		indexDefinitions.add(indexDefinition);
	}

	/**
	 * {@inheritDoc}
	 */
	public List<String> getFields() {
		final List<String> fields = new LinkedList<String>();
		for (final OIndexDefinition indexDefinition : indexDefinitions) {
			fields.addAll(indexDefinition.getFields());
		}
		return Collections.unmodifiableList(fields);
	}

	/**
	 * {@inheritDoc}
	 */
	public Object getDocumentValueToIndex(final ODocument iDocument) {
		final OCompositeKey compositeKey = new OCompositeKey();

		for (final OIndexDefinition indexDefinition : indexDefinitions) {
			final Comparable result = (Comparable) indexDefinition.getDocumentValueToIndex(iDocument);

			if (result == null)
				return null;

			compositeKey.addKey(result);
		}

		return compositeKey;
	}

	/**
	 * {@inheritDoc}
	 */
	public Comparable createValue(final List<?> params) {
		int currentParamIndex = 0;
		final OCompositeKey compositeKey = new OCompositeKey();

		for (final OIndexDefinition indexDefinition : indexDefinitions) {
			if (currentParamIndex + 1 > params.size())
				break;

			final int endIndex;
			if (currentParamIndex + indexDefinition.getParamCount() > params.size())
				endIndex = params.size();
			else
				endIndex = currentParamIndex + indexDefinition.getParamCount();

			final List<?> indexParams = params.subList(currentParamIndex, endIndex);
			currentParamIndex += indexDefinition.getParamCount();

			final Comparable keyValue = (Comparable) indexDefinition.createValue(indexParams);

			if (keyValue == null)
				return null;

			compositeKey.addKey(keyValue);
		}

		return compositeKey;
	}

	/**
	 * {@inheritDoc}
	 */
	public Comparable createValue(final Object... params) {
		return createValue(Arrays.asList(params));
	}

	/**
	 * {@inheritDoc}
	 */
	public int getParamCount() {
		int total = 0;
		for (final OIndexDefinition indexDefinition : indexDefinitions)
			total += indexDefinition.getParamCount();
		return total;
	}

	/**
	 * {@inheritDoc}
	 */
	public OType[] getTypes() {
		final List<OType> types = new LinkedList<OType>();
		for (final OIndexDefinition indexDefinition : indexDefinitions)
			Collections.addAll(types, indexDefinition.getTypes());

		return types.toArray(new OType[types.size()]);
	}

	@Override
	public boolean equals(final Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		final OCompositeIndexDefinition that = (OCompositeIndexDefinition) o;

		if (!className.equals(that.className))
			return false;
		if (!indexDefinitions.equals(that.indexDefinitions))
			return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = indexDefinitions.hashCode();
		result = 31 * result + className.hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "OCompositeIndexDefinition{" + "indexDefinitions=" + indexDefinitions + ", className='" + className + '\'' + '}';
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public ODocument toStream() {
		document.setInternalStatus(ORecordElement.STATUS.UNMARSHALLING);
		final List<ODocument> inds = new ArrayList<ODocument>(indexDefinitions.size());
		final List<String> indClasses = new ArrayList<String>(indexDefinitions.size());

		try {
			document.field("className", className);
			for (final OIndexDefinition indexDefinition : indexDefinitions) {
				final ODocument indexDocument = indexDefinition.toStream();
				inds.add(indexDocument);

				indClasses.add(indexDefinition.getClass().getName());
			}
			document.field("indexDefinitions", inds, OType.EMBEDDEDLIST);
			document.field("indClasses", indClasses, OType.EMBEDDEDLIST);
		} finally {
			document.setInternalStatus(ORecordElement.STATUS.LOADED);
		}
		return document;
	}

	/**
	 * {@inheritDoc}
	 */
	public String toCreateIndexDDL(final String indexName, final String indexType) {
		final StringBuilder ddl = new StringBuilder("create index ");
		ddl.append(indexName).append(" on ").append(className).append(" ( ");

		final Iterator<String> fieldIterator = getFields().iterator();
		if (fieldIterator.hasNext()) {
			ddl.append(fieldIterator.next());
			while (fieldIterator.hasNext()) {
				ddl.append(", ").append(fieldIterator.next());
			}
		}
		ddl.append(" ) ").append(indexType);
		return ddl.toString();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void fromStream() {
		try {
			className = document.field("className");

			final List<ODocument> inds = document.field("indexDefinitions");
			final List<String> indClasses = document.field("indClasses");

			indexDefinitions.clear();
			for (int i = 0; i < indClasses.size(); i++) {
				final Class<?> clazz = Class.forName(indClasses.get(i));
				final ODocument indDoc = inds.get(i);

				final OIndexDefinition indexDefinition = (OIndexDefinition) clazz.getDeclaredConstructor().newInstance();
				indexDefinition.fromStream(indDoc);

				indexDefinitions.add(indexDefinition);
			}

		} catch (final ClassNotFoundException e) {
			throw new OIndexException("Error during composite index deserialization", e);
		} catch (final NoSuchMethodException e) {
			throw new OIndexException("Error during composite index deserialization", e);
		} catch (final InvocationTargetException e) {
			throw new OIndexException("Error during composite index deserialization", e);
		} catch (final InstantiationException e) {
			throw new OIndexException("Error during composite index deserialization", e);
		} catch (final IllegalAccessException e) {
			throw new OIndexException("Error during composite index deserialization", e);
		}
	}
}

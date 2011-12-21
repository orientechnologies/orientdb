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

import java.util.List;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Presentation of index that is used information and contained in document
 * {@link com.orientechnologies.orient.core.metadata.schema.OClass} .
 * 
 * This object cannot be created directly, use {@link com.orientechnologies.orient.core.metadata.schema.OClass} manipulation method
 * instead.
 * 
 * @author Andrey Lomakin, Artem Orobets
 */
public interface OIndexDefinition extends OIndexCallback {
	/**
	 * @return Names of fields which given index is used to calculate key value. Order of fields is important.
	 */
	public List<String> getFields();

	/**
	 * @return Name of the class which this index belongs to.
	 */
	public String getClassName();

	/**
	 * {@inheritDoc}
	 */
	public boolean equals(Object index);

	/**
	 * {@inheritDoc}
	 */
	public int hashCode();

	/**
	 * {@inheritDoc}
	 */
	public String toString();

	/**
	 * Calculates key value by passed in parameters.
	 * 
	 * If it is impossible to calculate key value by given parameters <code>null</code> will be returned.
	 * 
	 * @param params
	 *          Parameters from which index key will be calculated.
	 * 
	 * @return Key value or null if calculation is impossible.
	 */
	public Object createValue(List<?> params);

	/**
	 * Calculates key value by passed in parameters.
	 * 
	 * If it is impossible to calculate key value by given parameters <code>null</code> will be returned.
	 * 
	 * 
	 * @param params
	 *          Parameters from which index key will be calculated.
	 * 
	 * @return Key value or null if calculation is impossible.
	 */
	public Object createValue(Object... params);

	/**
	 * Returns amount of parameters that are used to calculate key value. It does not mean that all parameters should be supplied. It
	 * only means that if you provide more parameters they will be ignored and will not participate in index key calculation.
	 * 
	 * @return Amount of that are used to calculate key value. Call result should be equals to {@code getTypes().length}.
	 */
	public int getParamCount();

	/**
	 * Return types of values from which index key consist. In case of index that is built on single document property value single
	 * array that contains property type will be returned. In case of composite indexes result will contain several key types.
	 * 
	 * @return Types of values from which index key consist.
	 */
	public OType[] getTypes();

	/**
	 * Serializes internal index state to document.
	 * 
	 * @return Document that contains internal index state.
	 */
	public ODocument toStream();

	/**
	 * Deserialize internal index state from document.
	 * 
	 * @param document
	 *          Serialized index presentation.
	 */
	public void fromStream(ODocument document);

	public String toCreateIndexDDL(String indexName, String indexType);
}

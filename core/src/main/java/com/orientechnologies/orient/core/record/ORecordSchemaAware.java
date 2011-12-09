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
package com.orientechnologies.orient.core.record;

import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * Generic record representation with a schema definition. The record has multiple fields. Fields are also called properties.
 */
public interface ORecordSchemaAware<T> extends ORecordInternal<T> {

	/**
	 * Returns the value of a field.
	 * 
	 * @param iFieldName
	 *          Field name
	 * @return Field value if exists, otherwise null
	 */
	public <RET> RET field(String iFieldName);

	/**
	 * Returns the value of a field forcing the return type. This is useful when you want avoid automatic conversions (for example
	 * record id -> record) or need expressly a conversion between types.
	 * 
	 * @param iFieldName
	 *          Field name
	 * @param iType
	 *          Type between the values defined in the {@link OType} enum
	 * @return Field value if exists, otherwise null
	 */
	public <RET> RET field(String iFieldName, OType iType);

	/**
	 * Sets the value for a field.
	 * 
	 * @param iFieldName
	 *          Field name
	 * @param iFieldValue
	 *          Field value to set
	 * @return The Record instance itself giving a "fluent interface". Useful to call multiple methods in chain.
	 */
	public ORecordSchemaAware<T> field(String iFieldName, Object iFieldValue);

	/**
	 * Sets the value for a field forcing the type.This is useful when you want avoid automatic conversions (for example record id ->
	 * record) or need expressly a conversion between types.
	 * 
	 * @param iFieldName
	 *          Field name
	 * @param iFieldValue
	 *          Field value to set
	 * @param iType
	 *          Type between the values defined in the {@link OType} enum
	 * @return
	 */
	public ORecordSchemaAware<T> field(String iFieldName, Object iFieldValue, OType iType);

	/**
	 * Removes a field. This operation does not set the field value to null but remove the field itself.
	 * 
	 * @param iFieldName
	 *          Field name
	 * @return The old value contained in the remove field
	 */
	public Object removeField(String iFieldName);

	/**
	 * Tells if a field is contained in current record.
	 * 
	 * @param iFieldName
	 *          Field name
	 * @return true if exists, otherwise false
	 */
	public boolean containsField(String iFieldName);

	/**
	 * Returns the number of fields present in memory.
	 * 
	 * @return Fields number
	 */
	public int fields();

	/**
	 * Returns the record's field names. The returned Set object is disconnected by internal representation, so changes don't apply to
	 * the record. If the fields are ordered the order is maintained also in the returning collection.
	 * 
	 * @return Set of string containing the field names
	 */
	public String[] fieldNames();

	/**
	 * Returns the record's field values. The returned object array is disconnected by internal representation, so changes don't apply
	 * to the record. If the fields are ordered the order is maintained also in the returning collection.
	 * 
	 * @return Object array of the field values
	 */
	public Object[] fieldValues();

	/**
	 * Returns the class name associated to the current record. Can be null. Call this method after a {@link #reset()} to re-associate
	 * the class.
	 * 
	 * @return Class name if any
	 */
	public String getClassName();

	/**
	 * Sets the class for the current record. If the class not exists, it will be created in transparent way as empty (no fields).
	 * 
	 * @param iClassName
	 *          Class name to set
	 */
	public void setClassName(String iClassName);

	/**
	 * Sets the class for the current record only if already exists in the schema.
	 * 
	 * @param iClassName
	 *          Class name to set
	 */
	public void setClassNameIfExists(String iClassName);

	/**
	 * Returns the schema class object for the record.
	 * 
	 * @return {@link OClass} instance or null if the record has no class associated
	 */
	public OClass getSchemaClass();

	/**
	 * Validates the record against the schema constraints if defined. If the record breaks the validation rules, then a
	 * {@link OValidationException} exception is thrown.
	 * 
	 * @throws OValidationException
	 */
	public void validate() throws OValidationException;
}

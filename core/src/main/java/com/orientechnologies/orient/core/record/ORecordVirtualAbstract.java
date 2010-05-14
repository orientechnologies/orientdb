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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * Abstract implementation for record-free implementations. The object can be reused across calls to the database by using the
 * reset() at every re-use. Field population and serialization occurs always in lazy way.
 */
@SuppressWarnings("unchecked")
public abstract class ORecordVirtualAbstract<T> extends ORecordSchemaAwareAbstract<T> {
	protected Map<String, T>			fields;
	protected Map<String, OType>	fieldTypes;

	public ORecordVirtualAbstract() {
	}

	public ORecordVirtualAbstract(byte[] iSource) {
		source = iSource;
	}

	public ORecordVirtualAbstract(String iClassName) {
		setClassName(iClassName);
	}

	public ORecordVirtualAbstract(ODatabaseRecord<?> iDatabase) {
		super(iDatabase);
	}

	public ORecordVirtualAbstract(ODatabaseRecord<?> iDatabase, String iClassName) {
		super(iDatabase);
		setClassName(iClassName);
	}

	/**
	 * Returns the forced field type if any.
	 * 
	 * @param iPropertyName
	 */
	public OType fieldType(final String iPropertyName) {
		return fieldTypes != null ? fieldTypes.get(iPropertyName) : null;
	}

	@Override
	public ORecordSchemaAwareAbstract<T> reset() {
		super.reset();
		if (fields != null)
			fields.clear();
		return this;
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();

		if (fields != null)
			for (Entry<String, T> field : fields.entrySet()) {
				if (field.getKey() != null)
					result += field.getKey().hashCode();

				if (field.getValue() != null)
					if (field.getValue() instanceof ORecord<?>)
						// AVOID TO GET THE HASH-CODE OF THE VALUE TO AVOID STACK OVERFLOW FOR CIRCULAR REFS
						result += 31 * ((ORecord<T>) field.getValue()).getIdentity().hashCode();
					else if (field.getValue() instanceof Collection<?>)
						// AVOID TO GET THE HASH-CODE OF THE VALUE TO AVOID STACK OVERFLOW FOR CIRCULAR REFS
						result += ((Collection<?>) field.getValue()).size() * 31;
					else
						result += field.getValue().hashCode();
			}

		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;

		if (getClass() != obj.getClass())
			return false;

		ORecordVirtualAbstract other = (ORecordVirtualAbstract) obj;

		if (!super.equals(obj))
			return false;

		if (!recordId.isValid()) {
			// NO PERSISTENT OBJECT: COMPARE EACH FIELDS
			if (fields == null || other.fields == null)
				// CAN'T COMPARE FIELDS: RETURN FALSE
				return false;

			if (fields.size() != other.fields.size())
				// FIELD SIZES ARE DIFFERENTS
				return false;

			String k;
			Object v;
			Object otherV;
			for (Entry<String, T> field : fields.entrySet()) {
				k = field.getKey();
				if (k != null && !other.containsField(k))
					// FIELD NOT PRESENT IN THE OTHER RECORD
					return false;

				v = fields.get(k);
				otherV = other.fields.get(k);
				if (v == null && otherV == null)
					continue;

				if (v == null && otherV != null || otherV == null && v != null)
					return false;

				if (!v.equals(otherV))
					return false;
			}
		}

		return true;
	}

	@Override
	protected void checkForFields() {
		if (fields == null)
			fields = new LinkedHashMap<String, T>();

		if (status == STATUS.LOADED && (fields == null || size() == 0))
			// POPULATE FIELDS LAZY
			deserializeFields();
	}
}

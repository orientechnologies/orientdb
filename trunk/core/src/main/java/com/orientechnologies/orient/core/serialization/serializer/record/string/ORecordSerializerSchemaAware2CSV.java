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
package com.orientechnologies.orient.core.serialization.serializer.record.string;

import java.text.DecimalFormatSymbols;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ORecordDocument;

public class ORecordSerializerSchemaAware2CSV extends ORecordSerializerCSVAbstract {
	public static final String	NAME	= "ORecordDocument2csv";

	@Override
	public ORecordSchemaAware<?> newObject(ODatabaseRecord<?> iDatabase, String iClassName) {
		return new ORecordDocument((ODatabaseDocument) iDatabase, iClassName);
	}

	@Override
	public String toString() {
		return NAME;
	}

	@Override
	protected String toString(ORecordSchemaAware<?> iRecord, final OUserObject2RecordHandler iObjHandler,
			final Map<ORecordInternal<?>, ORecordId> iMarshalledRecords) {
		final ORecordDocument record = (ORecordDocument) iRecord;

		// CHECK IF THE RECORD IS PENDING TO BE MARSHALLED
		if (iMarshalledRecords.containsKey(iRecord)) {
			return "-";
		} else
			iMarshalledRecords.put(iRecord, ORecordId.EMPTY_RECORD_ID);

		ODatabaseRecord<?> database = (ODatabaseRecord<?>) record.getDatabase();

		final StringBuilder buffer = new StringBuilder();

		if (iRecord.getClassName() != null) {
			// MARSHALL THE CLASSNAME
			buffer.append(iRecord.getClassName());
			buffer.append(CLASS_SEPARATOR);
		}

		OProperty prop;
		Object fieldValue;
		OType type;
		OClass linkedClass;
		OType linkedType;
		String fieldClassName;
		int i = 0;

		// MARSHALL ALL THE CONFIGURED FIELDS
		for (Entry<String, Object> f : record) {
			if (i > 0)
				buffer.append(RECORD_SEPARATOR);

			// SEARCH FOR A CONFIGURED PROPERTY
			prop = record.getSchemaClass() != null ? record.getSchemaClass().getProperty(f.getKey()) : null;
			fieldValue = f.getValue();
			fieldClassName = getClassName(fieldValue);

			if (prop != null) {
				// RECOGNIZED PROPERTY
				type = prop.getType();
				linkedClass = prop.getLinkedClass();
				linkedType = prop.getLinkedType();

			} else {
				// NOT FOUND: TRY TO DETERMINE THE TYPE FROM ITS CONTENT
				type = OType.STRING;
				linkedClass = null;
				linkedType = null;

				if (fieldValue instanceof Collection<?>) {
					Collection<?> coll = (Collection<?>) fieldValue;

					if (coll.size() > 0) {
						Object firstValue = coll.iterator().next();

						if (database != null
								&& (firstValue instanceof ORecordSchemaAware<?> || (database.getDatabaseOwner() instanceof ODatabaseObject && ((ODatabaseObject) database
										.getDatabaseOwner()).getEntityManager().getEntityClass(getClassName(firstValue)) != null))) {
							// LINK: GET THE CLASS
							linkedType = OType.LINK;
							linkedClass = getLinkInfo(database, getClassName(firstValue));

							if (coll instanceof Set<?>)
								type = OType.LINKSET;
							else
								type = OType.LINKLIST;

						} else {
							linkedType = OType.getTypeByClass(firstValue.getClass());

							if (linkedType != OType.LINK) {
								// EMBEDDED FOR SURE SINCE IT CONTAINS JAVA TYPES
								if (linkedType == null) {
									linkedType = OType.EMBEDDED;
									linkedClass = new OClass(firstValue.getClass());
								}

								if (coll instanceof Set<?>)
									type = OType.EMBEDDEDSET;
								else
									type = OType.EMBEDDEDLIST;
							}
						}
					} else
						type = OType.EMBEDDEDLIST;
				} else if (fieldValue instanceof Map<?, ?>) {
					Map<?, ?> map = (Map<?, ?>) fieldValue;
					if (map.size() > 0) {
						Object firstValue = map.values().iterator().next();

						linkedType = OType.getTypeByClass(firstValue.getClass());
						type = OType.EMBEDDEDMAP;
					}
				} else if (database != null && fieldValue instanceof ORecordSchemaAware<?>) {
					// DETERMINE THE FIELD TYPE
					type = OType.LINK;
					linkedClass = getLinkInfo(database, fieldClassName);
				} else if (database != null && database.getDatabaseOwner() instanceof ODatabaseObject
						&& ((ODatabaseObject) database.getDatabaseOwner()).getEntityManager().getEntityClass(fieldClassName) != null) {
					// DETERMINE THE FIELD TYPE
					type = OType.LINK;
					linkedClass = getLinkInfo(database, fieldClassName);
				}
			}

			fieldValue = fieldToStream((ORecordDocument) iRecord, iRecord.getDatabase(), iObjHandler, type, linkedClass, linkedType, f
					.getKey(), f.getValue(), iMarshalledRecords);

			buffer.append(f.getKey());
			buffer.append(FIELD_VALUE_SEPARATOR);
			if (fieldValue != null)
				buffer.append(fieldValue);

			i++;
		}

		return buffer.toString();
	}

	private String getClassName(final Object iValue) {
		if (iValue instanceof ORecordSchemaAware<?>)
			return ((ORecordSchemaAware<?>) iValue).getClassName();

		return iValue != null ? iValue.getClass().getSimpleName() : null;
	}

	private OClass getLinkInfo(ODatabaseComplex<?> database, String fieldClassName) {
		OClass linkedClass = database.getMetadata().getSchema().getClass(fieldClassName);

		if (database.getDatabaseOwner() instanceof ODatabaseObject) {
			ODatabaseObject dbo = (ODatabaseObject) database.getDatabaseOwner();
			if (linkedClass == null) {
				Class<?> entityClass = dbo.getEntityManager().getEntityClass(fieldClassName);
				if (entityClass != null) {
					// REGISTER IT
					linkedClass = database.getMetadata().getSchema().createClass(fieldClassName);
					database.getMetadata().getSchema().save();
				}
			}
		}

		return linkedClass;
	}

	@Override
	protected ORecordInternal<?> fromString(final ODatabaseRecord<?> iDatabase, String iContent, final ORecordInternal<?> iRecord) {
		iContent = iContent.trim();

		// UNMARSHALL THE CLASS NAME
		ORecordSchemaAware<?> record = (ORecordSchemaAware<?>) iRecord;

		int pos = iContent.indexOf(CLASS_SEPARATOR);
		if (pos > -1) {
			record.setClassName(iContent.substring(0, pos));
			iContent = iContent.substring(pos + 1);
		}

		String[] fields = split(iContent, RECORD_SEPARATOR_AS_CHAR);

		String field;
		String fieldName;
		String fieldValue;
		OType type;
		OClass linkedClass;
		OType linkedType;
		OProperty prop;

		DecimalFormatSymbols unusualSymbols = new DecimalFormatSymbols(Locale.getDefault());

		// UNMARSHALL ALL THE FIELDS
		for (int i = 0; i < fields.length; ++i) {
			field = fields[i].trim();

			pos = field.indexOf(FIELD_VALUE_SEPARATOR);
			if (pos > -1) {
				// GET THE FIELD NAME
				fieldName = field.substring(0, pos);

				// GET THE FIELD VALUE
				fieldValue = field.length() > pos + 1 ? field.substring(pos + 1) : null;

				// SEARCH FOR A CONFIGURED PROPERTY
				prop = record.getSchemaClass() != null ? record.getSchemaClass().getProperty(fieldName) : null;
				if (prop != null) {
					// RECOGNIZED PROPERTY
					type = prop.getType();
					linkedClass = prop.getLinkedClass();
					linkedType = prop.getLinkedType();

				} else {
					type = OType.STRING;
					linkedClass = null;
					linkedType = null;

					// NOT FOUND: TRY TO DETERMINE THE TYPE FROM ITS CONTENT
					if (fieldValue != null) {
						if (fieldValue.charAt(0) == '[' && fieldValue.charAt(fieldValue.length() - 1) == ']') {
							type = OType.EMBEDDEDLIST;

							String value = fieldValue.substring(1, fieldValue.length() - 1);

							if (value.length() > 0) {
								if (value.contains(ORID.SEPARATOR)) {
									type = OType.LINKLIST;
									linkedType = OType.LINK;

									// GET THE CLASS NAME IF ANY
									int classSeparatorPos = value.indexOf(CLASS_SEPARATOR);
									if (classSeparatorPos > -1) {
										String className = value.substring(1, classSeparatorPos);
										if (className != null)
											linkedClass = iDatabase.getMetadata().getSchema().getClass(className);
									}
								} else if (Character.isDigit(value.charAt(0)) || value.charAt(0) == '+' || value.charAt(0) == '-') {
									linkedType = getNumber(unusualSymbols, value);
								} else if (value.charAt(0) == '\'' || value.charAt(0) == '"')
									linkedType = OType.STRING;
								else
									linkedType = OType.EMBEDDED;
							} else
								linkedType = OType.STRING;
						} else if (fieldValue.startsWith(LINK))
							type = OType.LINK;
						else if (Character.isDigit(fieldValue.charAt(0))) {
							type = getNumber(unusualSymbols, fieldValue);
						}
					}
				}

				record.field(fieldName, fieldFromStream(iRecord.getDatabase(), type, linkedClass, linkedType, fieldName, fieldValue));
			}
		}

		return iRecord;
	}

	public static OType getNumber(DecimalFormatSymbols unusualSymbols, String value) {
		boolean integer = true;
		char c;

		for (int index = 1; index < value.length(); ++index) {
			c = value.charAt(index);
			if (c < 0 || c > 9)
				if (c == unusualSymbols.getDecimalSeparator())
					integer = false;
				else {
					return OType.STRING;
				}
		}

		return integer ? OType.INTEGER : OType.FLOAT;
	}
}

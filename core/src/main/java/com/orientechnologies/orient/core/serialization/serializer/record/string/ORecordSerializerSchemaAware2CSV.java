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

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

public class ORecordSerializerSchemaAware2CSV extends ORecordSerializerCSVAbstract {
	public static final String														NAME			= "ORecordDocument2csv";
	public static final ORecordSerializerSchemaAware2CSV	INSTANCE	= new ORecordSerializerSchemaAware2CSV();

	@Override
	public ORecordSchemaAware<?> newObject(ODatabaseRecord iDatabase, String iClassName) {
		return new ODocument(iDatabase, iClassName);
	}

	@Override
	public String toString() {
		return NAME;
	}

	@Override
	protected StringBuilder toString(ORecordInternal<?> iRecord, final StringBuilder iOutput, final String iFormat,
			final OUserObject2RecordHandler iObjHandler, final Set<Integer> iMarshalledRecords, final boolean iOnlyDelta) {
		if (!(iRecord instanceof ODocument))
			throw new OSerializationException("Cannot marshall a record of type " + iRecord.getClass().getSimpleName() + " to CSV");

		final ODocument record = (ODocument) iRecord;

		// CHECK IF THE RECORD IS PENDING TO BE MARSHALLED
		final Integer identityRecord = System.identityHashCode(record);
		if (iMarshalledRecords != null)
			if (iMarshalledRecords.contains(identityRecord)) {
				return iOutput;
			} else
				iMarshalledRecords.add(identityRecord);

		final ODatabaseRecord database = ODatabaseRecordThreadLocal.INSTANCE.get();

		if (!iOnlyDelta && record.getSchemaClass() != null) {
			// MARSHALL THE CLASSNAME
			iOutput.append(record.getSchemaClass().getStreamableName());
			iOutput.append(OStringSerializerHelper.CLASS_SEPARATOR);
		}

		OProperty prop;
		OType type;
		OClass linkedClass;
		OType linkedType;
		String fieldClassName;
		int i = 0;

		final String[] fieldNames = iOnlyDelta && record.isTrackingChanges() ? record.getDirtyFields() : record.fieldNames();

		// MARSHALL ALL THE FIELDS OR DELTA IF TRACKING IS ENABLED
		for (String fieldName : fieldNames) {
			Object fieldValue = record.rawField(fieldName);
			if (i > 0)
				iOutput.append(OStringSerializerHelper.RECORD_SEPARATOR);

			// SEARCH FOR A CONFIGURED PROPERTY
			prop = record.getSchemaClass() != null ? record.getSchemaClass().getProperty(fieldName) : null;
			fieldClassName = getClassName(fieldValue);

			type = record.fieldType(fieldName);
			linkedClass = null;
			linkedType = null;

			if (prop != null) {
				// RECOGNIZED PROPERTY
				type = prop.getType();
				linkedClass = prop.getLinkedClass();
				linkedType = prop.getLinkedType();

			} else if (fieldValue != null) {
				// NOT FOUND: TRY TO DETERMINE THE TYPE FROM ITS CONTENT
				if (type == null) {
					if (fieldValue.getClass() == byte[].class)
						type = OType.BINARY;
					else if (database != null && fieldValue instanceof ORecord<?>) {
						if (type == null)
							// DETERMINE THE FIELD TYPE
							if (fieldValue instanceof ODocument && ((ODocument) fieldValue).hasOwners())
								type = OType.EMBEDDED;
							else
								type = OType.LINK;

						linkedClass = getLinkInfo(database, fieldClassName);
					} else if (fieldValue instanceof ORID)
						// DETERMINE THE FIELD TYPE
						type = OType.LINK;

					else if (database != null && database.getDatabaseOwner() instanceof ODatabaseObject
							&& ((ODatabaseObject) database.getDatabaseOwner()).getEntityManager().getEntityClass(fieldClassName) != null) {
						// DETERMINE THE FIELD TYPE
						type = OType.LINK;
						linkedClass = getLinkInfo(database, fieldClassName);
					} else if (fieldValue instanceof Date)
						type = OType.DATETIME;
					else if (fieldValue instanceof String)
						type = OType.STRING;
					else if (fieldValue instanceof Integer)
						type = OType.INTEGER;
					else if (fieldValue instanceof Long)
						type = OType.LONG;
					else if (fieldValue instanceof Float)
						type = OType.FLOAT;
					else if (fieldValue instanceof Short)
						type = OType.SHORT;
					else if (fieldValue instanceof Byte)
						type = OType.BYTE;
					else if (fieldValue instanceof Double)
						type = OType.DOUBLE;
				}

				if (fieldValue instanceof Collection<?> || fieldValue.getClass().isArray()) {
					int size = OMultiValue.getSize(fieldValue);

					Boolean autoConvertLinks = null;
					if (fieldValue instanceof ORecordLazyMultiValue) {
						autoConvertLinks = ((ORecordLazyMultiValue) fieldValue).isAutoConvertToRecord();
						if (autoConvertLinks)
							// DISABLE AUTO CONVERT
							((ORecordLazyMultiValue) fieldValue).setAutoConvertToRecord(false);
					}

					if (size > 0) {
						final Object firstValue = OMultiValue.getFirstValue(fieldValue);

						if (firstValue != null) {
							if (firstValue instanceof ORID) {
								linkedClass = null;
								linkedType = OType.LINK;
								if (fieldValue instanceof Set<?>)
									type = OType.LINKSET;
								else
									type = OType.LINKLIST;
							} else if (database != null
									&& (firstValue instanceof ORecordSchemaAware<?> || (database.getDatabaseOwner() instanceof ODatabaseObject && ((ODatabaseObject) database
											.getDatabaseOwner()).getEntityManager().getEntityClass(getClassName(firstValue)) != null))) {
								linkedClass = getLinkInfo(database, getClassName(firstValue));
								if (type == null) {
									// LINK: GET THE CLASS
									linkedType = OType.LINK;

									if (fieldValue instanceof Set<?>)
										type = OType.LINKSET;
									else
										type = OType.LINKLIST;
								} else
									linkedType = OType.EMBEDDED;
							} else {
								// EMBEDDED COLLECTION
								if (firstValue instanceof ODocument && ((ODocument) firstValue).hasOwners())
									linkedType = OType.EMBEDDED;
								else if (firstValue instanceof Enum<?>)
									linkedType = OType.STRING;
								else {
									linkedType = OType.getTypeByClass(firstValue.getClass());

									if (linkedType != OType.LINK) {
										// EMBEDDED FOR SURE SINCE IT CONTAINS JAVA TYPES
										if (linkedType == null) {
											linkedType = OType.EMBEDDED;
											// linkedClass = new OClass(firstValue.getClass());
										}
									}
								}

								if (type == null)
									if (fieldValue instanceof Set<?>)
										type = OType.EMBEDDEDSET;
									else
										type = OType.EMBEDDEDLIST;
							}
						}
					} else if (type == null)
						type = OType.EMBEDDEDLIST;

					if (fieldValue instanceof ORecordLazyMultiValue && autoConvertLinks) {
						// REPLACE PREVIOUS SETTINGS
						((ORecordLazyMultiValue) fieldValue).setAutoConvertToRecord(true);
					}

				} else if (fieldValue instanceof Map<?, ?> && type == null)
					type = OType.EMBEDDEDMAP;
			}

			if (type == OType.TRANSIENT)
				// TRANSIENT FIELD
				continue;

			if (type == null)
				type = OType.EMBEDDED;

			iOutput.append(fieldName);
			iOutput.append(FIELD_VALUE_SEPARATOR);
			fieldToStream((ODocument) iRecord, iRecord.getDatabase(), iOutput, iObjHandler, type, linkedClass, linkedType, fieldName,
					fieldValue, iMarshalledRecords, true);

			i++;
		}

		if (iMarshalledRecords != null)
			iMarshalledRecords.remove(identityRecord);

		// GET THE OVERSIZE IF ANY
		final float overSize;
		if (record.getSchemaClass() != null)
			// GET THE CONFIGURED OVERSIZE SETTED PER CLASS
			overSize = record.getSchemaClass().getOverSize();
		else
			overSize = 0;

		// APPEND BLANKS IF NEEDED
		final int newSize;
		if (record.hasOwners())
			// EMBEDDED: GET REAL SIZE
			newSize = iOutput.length();
		else if (record.getSize() == iOutput.length())
			// IDENTICAL! DO NOTHING
			newSize = record.getSize();
		else if (record.getSize() > iOutput.length()) {
			// APPEND EXTRA SPACES TO FILL ALL THE AVAILABLE SPACE AND AVOID FRAGMENTATION
			newSize = record.getSize();
		} else if (overSize > 0) {
			// APPEND EXTRA SPACES TO GET A LARGER iOutput
			newSize = (int) (iOutput.length() * overSize);
		} else
			// NO OVERSIZE
			newSize = iOutput.length();

		if (newSize > iOutput.length())
			for (int b = iOutput.length(); b < newSize; ++b)
				iOutput.append(' ');

		return iOutput;
	}

	private String getClassName(final Object iValue) {
		if (iValue instanceof ORecordSchemaAware<?>)
			return ((ORecordSchemaAware<?>) iValue).getClassName();

		return iValue != null ? iValue.getClass().getSimpleName() : null;
	}

	private OClass getLinkInfo(final ODatabaseComplex<?> iDatabase, final String iFieldClassName) {
		OClass linkedClass = iDatabase.getMetadata().getSchema().getClass(iFieldClassName);

		if (iDatabase.getDatabaseOwner() instanceof ODatabaseObject) {
			ODatabaseObject dbo = (ODatabaseObject) iDatabase.getDatabaseOwner();
			if (linkedClass == null) {
				Class<?> entityClass = dbo.getEntityManager().getEntityClass(iFieldClassName);
				if (entityClass != null)
					// REGISTER IT
					linkedClass = iDatabase.getMetadata().getSchema().createClass(iFieldClassName);
			}
		}

		return linkedClass;
	}

	@Override
	protected ORecordInternal<?> fromString(final ODatabaseRecord iDatabase, String iContent, final ORecordInternal<?> iRecord) {
		iContent = iContent.trim();

		if (iContent.length() == 0)
			return iRecord;

		// UNMARSHALL THE CLASS NAME
		final ODocument record = (ODocument) iRecord;

		final int posFirstValue = iContent.indexOf(OStringSerializerHelper.ENTRY_SEPARATOR);
		int pos = iContent.indexOf(OStringSerializerHelper.CLASS_SEPARATOR);
		if (pos > -1 && (pos < posFirstValue || posFirstValue == -1)) {
			record.setClassNameIfExists(iContent.substring(0, pos));
			iContent = iContent.substring(pos + 1);
		} else
			record.setClassNameIfExists(null);

		final List<String> fields = OStringSerializerHelper.smartSplit(iContent, OStringSerializerHelper.RECORD_SEPARATOR);

		String field;
		String fieldName = null;
		String fieldValue;
		OType type = null;
		OClass linkedClass;
		OType linkedType;
		OProperty prop;

		// UNMARSHALL ALL THE FIELDS
		for (int i = 0; i < fields.size(); ++i) {
			field = fields.get(i).trim();

			boolean uncertainType = false;

			try {
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
						// SCHEMA PROPERTY NOT FOUND FOR THIS FIELD: TRY TO AUTODETERMINE THE BEST TYPE
						type = record.fieldType(fieldName);
						linkedClass = null;
						linkedType = null;

						// NOT FOUND: TRY TO DETERMINE THE TYPE FROM ITS CONTENT
						if (fieldValue != null && type == null) {
							if (fieldValue.length() > 1 && fieldValue.charAt(0) == '"' && fieldValue.charAt(fieldValue.length() - 1) == '"') {
								type = OType.STRING;
							} else if (fieldValue.charAt(0) == OStringSerializerHelper.COLLECTION_BEGIN
									&& fieldValue.charAt(fieldValue.length() - 1) == OStringSerializerHelper.COLLECTION_END) {
								type = OType.EMBEDDEDLIST;

								final String value = fieldValue.substring(1, fieldValue.length() - 1);

								if (!value.isEmpty()) {
									if (value.charAt(0) == OStringSerializerHelper.LINK) {
										type = OType.LINKLIST;
										linkedType = OType.LINK;

										// GET THE CLASS NAME IF ANY
										int classSeparatorPos = value.indexOf(OStringSerializerHelper.CLASS_SEPARATOR);
										if (classSeparatorPos > -1) {
											String className = value.substring(1, classSeparatorPos);
											if (className != null)
												linkedClass = iDatabase.getMetadata().getSchema().getClass(className);
										}
									} else if (value.charAt(0) == OStringSerializerHelper.EMBEDDED_BEGIN) {
										linkedType = OType.EMBEDDED;
									} else if (value.charAt(0) == OStringSerializerHelper.CUSTOM_TYPE) {
										linkedType = OType.CUSTOM;
									} else if (Character.isDigit(value.charAt(0)) || value.charAt(0) == '+' || value.charAt(0) == '-') {
										String[] items = value.split(",");
										linkedType = getType(items[0]);
									} else if (value.charAt(0) == '\'' || value.charAt(0) == '"')
										linkedType = OType.STRING;
								} else
									uncertainType = true;

							} else if (fieldValue.charAt(0) == OStringSerializerHelper.MAP_BEGIN
									&& fieldValue.charAt(fieldValue.length() - 1) == OStringSerializerHelper.MAP_END) {
								type = OType.EMBEDDEDMAP;
							} else if (fieldValue.charAt(0) == OStringSerializerHelper.LINK)
								type = OType.LINK;
							else if (fieldValue.charAt(0) == OStringSerializerHelper.EMBEDDED_BEGIN) {
								// TEMPORARY PATCH
								if (fieldValue.startsWith("(ORIDs"))
									type = OType.LINKSET;
								else
									type = OType.EMBEDDED;
							} else if (fieldValue.equals("true") || fieldValue.equals("false"))
								type = OType.BOOLEAN;
							else
								type = getType(fieldValue);
						}
					}

					if (type == OType.EMBEDDEDLIST || type == OType.EMBEDDEDSET || type == OType.EMBEDDEDMAP || type == OType.EMBEDDED)
						// SAVE THE TYPE AS EMBEDDED
						record.field(fieldName, fieldFromStream(iRecord, type, linkedClass, linkedType, fieldName, fieldValue), type);
					else
						record.field(fieldName, fieldFromStream(iRecord, type, linkedClass, linkedType, fieldName, fieldValue));

					if (uncertainType)
						record.setFieldType(fieldName, null);
				}
			} catch (Exception e) {
				OLogManager.instance().exception("Error on unmarshalling field '%s' with value: ", e, OSerializationException.class,
						fieldName, field);
			}
		}

		return iRecord;
	}
}

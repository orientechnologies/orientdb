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

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

public class ORecordSerializerSchemaAware2CSV extends ORecordSerializerCSVAbstract {
	public static final String														NAME			= "ORecordDocument2csv";
	public static final ORecordSerializerSchemaAware2CSV	INSTANCE	= new ORecordSerializerSchemaAware2CSV();

	@Override
	public ORecordSchemaAware<?> newObject(ODatabaseRecord<?> iDatabase, String iClassName) {
		return new ODocument(iDatabase, iClassName);
	}

	@Override
	public String toString() {
		return NAME;
	}

	@Override
	protected String toString(ORecordInternal<?> iRecord, final String iFormat, final OUserObject2RecordHandler iObjHandler,
			final Set<Integer> iMarshalledRecords) {
		if (!(iRecord instanceof ODocument))
			throw new OSerializationException("Can't marshall a record of type " + iRecord.getClass().getSimpleName() + " to CSV");

		final ODocument record = (ODocument) iRecord;

		// CHECK IF THE RECORD IS PENDING TO BE MARSHALLED
		final Integer identityRecord = System.identityHashCode(record);
		if (iMarshalledRecords.contains(identityRecord)) {
			return "";
		} else
			iMarshalledRecords.add(identityRecord);

		ODatabaseRecord<?> database = record.getDatabase();

		final StringBuilder buffer = new StringBuilder();

		if (record.getClassName() != null) {
			// MARSHALL THE CLASSNAME
			buffer.append(record.getClassName());
			buffer.append(OStringSerializerHelper.CLASS_SEPARATOR);
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
				buffer.append(OStringSerializerHelper.RECORD_SEPARATOR);

			// SEARCH FOR A CONFIGURED PROPERTY
			prop = record.getSchemaClass() != null ? record.getSchemaClass().getProperty(f.getKey()) : null;
			fieldValue = f.getValue();
			fieldClassName = getClassName(fieldValue);

			type = record.fieldType(f.getKey());
			linkedClass = null;
			linkedType = null;

			if (prop != null) {
				// RECOGNIZED PROPERTY
				type = prop.getType();
				linkedClass = prop.getLinkedClass();
				linkedType = prop.getLinkedType();

			} else if (fieldValue != null) {

				// NOT FOUND: TRY TO DETERMINE THE TYPE FROM ITS CONTENT
				if (fieldValue instanceof Collection<?> || fieldValue.getClass().isArray()) {
					final Collection<?> coll = fieldValue instanceof Collection<?> ? (Collection<?>) fieldValue : null;

					int size = coll != null ? coll.size() : Array.getLength(fieldValue);

					if (size > 0) {
						Object firstValue = coll != null ? coll.iterator().next() : Array.get(fieldValue, 0);

						if (database != null
								&& (firstValue instanceof ORID || firstValue instanceof ORecordSchemaAware<?> || (database.getDatabaseOwner() instanceof ODatabaseObject && ((ODatabaseObject) database
										.getDatabaseOwner()).getEntityManager().getEntityClass(getClassName(firstValue)) != null))) {
							linkedClass = getLinkInfo(database, getClassName(firstValue));
							if (type == null) {
								// LINK: GET THE CLASS
								linkedType = OType.LINK;

								if (coll instanceof Set<?>)
									type = OType.LINKSET;
								else
									type = OType.LINKLIST;
							} else
								linkedType = OType.EMBEDDED;
						} else {
							linkedType = OType.getTypeByAssignability(firstValue.getClass());

							if (linkedType != OType.LINK) {
								// EMBEDDED FOR SURE SINCE IT CONTAINS JAVA TYPES
								if (linkedType == null) {
									linkedType = OType.EMBEDDED;
									// linkedClass = new OClass(firstValue.getClass());
								}

								if (type == null)
									if (coll instanceof Set<?>)
										type = OType.EMBEDDEDSET;
									else
										type = OType.EMBEDDEDLIST;
							}
						}
					} else if (type == null)
						type = OType.EMBEDDEDLIST;

				} else if (fieldValue instanceof Map<?, ?>) {
					if (type == null)
						type = OType.EMBEDDEDMAP;

					Map<?, ?> map = (Map<?, ?>) fieldValue;
					if (map.size() > 0) {
						Object firstValue = map.values().iterator().next();

						if (database != null
								&& (firstValue instanceof ORecordSchemaAware<?> || (database.getDatabaseOwner() instanceof ODatabaseObject && ((ODatabaseObject) database
										.getDatabaseOwner()).getEntityManager().getEntityClass(getClassName(firstValue)) != null))) {
							// LINK: GET THE CLASS
							linkedType = type == OType.EMBEDDEDLIST || type == OType.EMBEDDEDSET || type == OType.EMBEDDEDMAP ? OType.EMBEDDED
									: OType.LINK;
							linkedClass = getLinkInfo(database, getClassName(firstValue));
						} else
							linkedType = OType.getTypeByClass(firstValue.getClass());
					}
				} else if (database != null && fieldValue instanceof ODocument) {
					if (type == null)
						// DETERMINE THE FIELD TYPE
						if (((ODocument) fieldValue).getOwner() == null)
							type = OType.LINK;
						else
							type = OType.EMBEDDED;

					linkedClass = getLinkInfo(database, fieldClassName);
				} else if (fieldValue instanceof ORID) {
					if (type == null)
						// DETERMINE THE FIELD TYPE
						type = OType.LINK;

				} else if (database != null && database.getDatabaseOwner() instanceof ODatabaseObject
						&& ((ODatabaseObject) database.getDatabaseOwner()).getEntityManager().getEntityClass(fieldClassName) != null) {
					// DETERMINE THE FIELD TYPE
					if (type == null)
						type = OType.LINK;
					linkedClass = getLinkInfo(database, fieldClassName);
				} else if (fieldValue instanceof Date) {
					if (type == null)
						type = OType.DATE;
				} else if (fieldValue instanceof String)
					type = OType.STRING;
			}

			if (type == null)
				type = OType.EMBEDDED;

			fieldValue = fieldToStream((ODocument) iRecord, iRecord.getDatabase(), iObjHandler, type, linkedClass, linkedType,
					f.getKey(), f.getValue(), iMarshalledRecords);

			buffer.append(f.getKey());
			buffer.append(FIELD_VALUE_SEPARATOR);
			if (fieldValue != null)
				buffer.append(fieldValue);

			i++;
		}

		iMarshalledRecords.remove(identityRecord);

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

		if (iContent.length() == 0)
			return iRecord;

		// UNMARSHALL THE CLASS NAME
		final ORecordSchemaAware<?> record = (ORecordSchemaAware<?>) iRecord;

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
						linkedClass = null;
						linkedType = null;

						// NOT FOUND: TRY TO DETERMINE THE TYPE FROM ITS CONTENT
						if (fieldValue != null) {
							if (fieldValue.length() > 1 && fieldValue.charAt(0) == '"' && fieldValue.charAt(fieldValue.length() - 1) == '"') {
								type = OType.STRING;
							} else if (fieldValue.charAt(0) == OStringSerializerHelper.COLLECTION_BEGIN
									&& fieldValue.charAt(fieldValue.length() - 1) == OStringSerializerHelper.COLLECTION_END) {
								type = OType.EMBEDDEDLIST;

								String value = fieldValue.substring(1, fieldValue.length() - 1);

								if (value.length() > 0) {
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
									} else if (value.charAt(0) == OStringSerializerHelper.EMBEDDED) {
										linkedType = OType.EMBEDDED;
									} else if (Character.isDigit(value.charAt(0)) || value.charAt(0) == '+' || value.charAt(0) == '-') {
										String[] items = value.split(",");
										linkedType = getNumber(items[0]);
									} else if (value.charAt(0) == '\'' || value.charAt(0) == '"')
										linkedType = OType.STRING;
								}

							} else if (fieldValue.charAt(0) == OStringSerializerHelper.MAP_BEGIN
									&& fieldValue.charAt(fieldValue.length() - 1) == OStringSerializerHelper.MAP_END) {
								type = OType.EMBEDDEDMAP;
							} else if (fieldValue.charAt(0) == OStringSerializerHelper.LINK)
								type = OType.LINK;
							else if (fieldValue.charAt(0) == OStringSerializerHelper.EMBEDDED)
								type = OType.EMBEDDED;
							else if (fieldValue.equals("true") || fieldValue.equals("false"))
								type = OType.BOOLEAN;
							else
								type = getNumber(fieldValue);
						}
					}

					record.field(fieldName, fieldFromStream(iRecord, type, linkedClass, linkedType, fieldName, fieldValue));
				}
			} catch (Exception e) {
				OLogManager.instance().exception("Error on unmarshalling field '%s'", e, OSerializationException.class, fieldName);
			}
		}

		record.unsetDirty();

		return iRecord;
	}
}

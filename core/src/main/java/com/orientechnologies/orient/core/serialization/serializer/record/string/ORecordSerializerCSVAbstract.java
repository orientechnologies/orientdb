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
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.entity.OEntityManagerInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.object.OObjectSerializerHelper;

@SuppressWarnings("unchecked")
public abstract class ORecordSerializerCSVAbstract extends ORecordSerializerStringAbstract {
	public static final String	FIELD_VALUE_SEPARATOR	= ":";

	protected abstract ORecordSchemaAware<?> newObject(ODatabaseRecord<?> iDatabase, String iClassName);

	public Object fieldFromStream(final ODatabaseRecord<?> iDatabase, final OType iType, OClass iLinkedClass, OType iLinkedType,
			final String iName, final String iValue, final DecimalFormatSymbols iUnusualSymbols) {

		if (iValue == null)
			return null;

		switch (iType) {
		case EMBEDDEDLIST:
		case EMBEDDEDSET: {
			if (iValue.length() == 0)
				return null;

			// REMOVE BEGIN & END COLLECTIONS CHARACTERS
			String value = iValue.substring(1, iValue.length() - 1);

			Collection<Object> coll = iType == OType.EMBEDDEDLIST ? new ArrayList<Object>() : new HashSet<Object>();

			if (value.length() == 0)
				return coll;

			String[] items = OStringSerializerHelper.split(value, OStringSerializerHelper.RECORD_SEPARATOR_AS_CHAR);

			if (iLinkedClass != null)
				// EMBEDDED OBJECTS
				for (String item : items) {
					coll.add(fromString(iDatabase, item, new ODocument(iDatabase, iLinkedClass.getName())));
				}
			else {
				if (iLinkedType == null)
					throw new IllegalArgumentException(
							"Linked type can't be null. Probably the serialized type has not stored the type along with data");

				// EMBEDDED LITERALS
				for (String item : items) {
					coll.add(OStringSerializerHelper.fieldTypeFromStream(iLinkedType, item));
				}
			}

			return coll;
		}

		case LINKLIST:
		case LINKSET: {
			if (iValue.length() == 0)
				return null;

			// REMOVE BEGIN & END COLLECTIONS CHARACTERS
			String value = iValue.substring(1, iValue.length() - 1);

			Collection<Object> coll = iType == OType.LINKLIST ? new ArrayList<Object>() : new HashSet<Object>();

			if (value.length() == 0)
				return coll;

			String[] items = OStringSerializerHelper.split(value, OStringSerializerHelper.RECORD_SEPARATOR_AS_CHAR);

			for (String item : items) {
				// GET THE CLASS NAME IF ANY
				int classSeparatorPos = value.indexOf(OStringSerializerHelper.CLASS_SEPARATOR);
				if (classSeparatorPos > -1) {
					String className = value.substring(1, classSeparatorPos);
					if (className != null) {
						iLinkedClass = iDatabase.getMetadata().getSchema().getClass(className);
						item = item.substring(classSeparatorPos + 1);
					}
				} else
					item = item.substring(1);

				coll.add(new ODocument(iDatabase, iLinkedClass != null ? iLinkedClass.getName() : null, new ORecordId(item)));
			}

			return coll;
		}
		case EMBEDDEDMAP: {
			if (iValue.length() == 0)
				return null;

			// REMOVE BEGIN & END MAP CHARACTERS
			String value = iValue.substring(1, iValue.length() - 1);

			Map<String, Object> map = new HashMap<String, Object>();

			if (value.length() == 0)
				return map;

			String[] items = OStringSerializerHelper.split(value, OStringSerializerHelper.RECORD_SEPARATOR_AS_CHAR);

			// EMBEDDED LITERALS
			String[] entry;
			String mapValue;
			for (String item : items) {
				if (item != null && item.length() > 0) {
					entry = item.split(OStringSerializerHelper.ENTRY_SEPARATOR);
					if (entry.length > 0) {
						mapValue = entry[1];

						if (mapValue.contains(ORID.SEPARATOR)) {
							iLinkedType = OType.LINK;

							// GET THE CLASS NAME IF ANY
							int classSeparatorPos = value.indexOf(OStringSerializerHelper.CLASS_SEPARATOR);
							if (classSeparatorPos > -1) {
								String className = value.substring(1, classSeparatorPos);
								if (className != null)
									iLinkedClass = iDatabase.getMetadata().getSchema().getClass(className);
							}
						} else if (Character.isDigit(mapValue.charAt(0)) || mapValue.charAt(0) == '+' || mapValue.charAt(0) == '-') {
							iLinkedType = getNumber(iUnusualSymbols, mapValue);
						} else if (mapValue.charAt(0) == '\'' || mapValue.charAt(0) == '"')
							iLinkedType = OType.STRING;
						else
							iLinkedType = OType.EMBEDDED;

						map.put((String) OStringSerializerHelper.fieldTypeFromStream(OType.STRING, entry[0]), OStringSerializerHelper
								.fieldTypeFromStream(iLinkedType, mapValue));
					}

				}
			}

			return map;
		}
		case LINK:
			int pos = iValue.indexOf(OStringSerializerHelper.CLASS_SEPARATOR);
			if (pos > -1)
				iLinkedClass = iDatabase.getMetadata().getSchema().getClass(iValue.substring(OStringSerializerHelper.LINK.length(), pos));
			else
				pos = 0;

			if (iLinkedClass == null)
				throw new IllegalArgumentException("Linked class not specified in ORID field: " + iValue);

			ORecordId recId = new ORecordId(iValue.substring(pos + 1));
			return new ODocument(iDatabase, iLinkedClass.getName(), recId);

		default:
			return OStringSerializerHelper.fieldTypeFromStream(iType, iValue);
		}
	}

	public String fieldToStream(final ODocument iRecord, final ODatabaseRecord iDatabase,
			final OUserObject2RecordHandler iObjHandler, final OType iType, final OClass iLinkedClass, final OType iLinkedType,
			final String iName, final Object iValue, final Map<ORecordInternal<?>, ORecordId> iMarshalledRecords) {
		StringBuilder buffer = new StringBuilder();

		switch (iType) {
		case EMBEDDED:
			if (iValue instanceof ODocument)
				buffer.append(toString((ODocument) iValue, null, iObjHandler, iMarshalledRecords));
			else if (iValue != null)
				buffer.append(iValue.toString());
			break;

		case LINK: {
			linkToStream(buffer, iRecord, (ORecordSchemaAware<?>) iValue);
			break;
		}

		case EMBEDDEDLIST:
		case EMBEDDEDSET: {
			buffer.append(OStringSerializerHelper.COLLECTION_BEGIN);

			int size = iValue instanceof Collection<?> ? ((Collection<Object>) iValue).size() : Array.getLength(iValue);
			Iterator<Object> iterator = iValue instanceof Collection<?> ? ((Collection<Object>) iValue).iterator() : null;
			Object o;

			for (int i = 0; i < size; ++i) {
				if (iValue instanceof Collection<?>)
					o = iterator.next();
				else
					o = Array.get(iValue, i);

				if (iLinkedClass != null) {
					// EMBEDDED OBJECTS
					ODocument record;

					if (i > 0)
						buffer.append(OStringSerializerHelper.RECORD_SEPARATOR);

					if (o != null) {
						if (o instanceof ODocument)
							record = (ODocument) o;
						else
							record = OObjectSerializerHelper.toStream(o, new ODocument(o.getClass().getSimpleName()),
									iDatabase instanceof ODatabaseObjectTx ? ((ODatabaseObjectTx) iDatabase).getEntityManager()
											: OEntityManagerInternal.INSTANCE, iLinkedClass, iObjHandler != null ? iObjHandler
											: new OUserObject2RecordHandler() {

												public Object getUserObjectByRecord(ORecordInternal<?> iRecord) {
													return iRecord;
												}

												public ORecordInternal<?> getRecordByUserObject(Object iPojo, boolean iIsMandatory) {
													return new ODocument(iLinkedClass);
												}
											});

						buffer.append(toString(record, null, iObjHandler, iMarshalledRecords));
					}
				} else {
					// EMBEDDED LITERALS
					if (i > 0)
						buffer.append(OStringSerializerHelper.RECORD_SEPARATOR);

					buffer.append(OStringSerializerHelper.fieldTypeToString(iLinkedType, o));
				}
			}

			buffer.append(OStringSerializerHelper.COLLECTION_END);
			break;
		}

		case EMBEDDEDMAP: {
			buffer.append(OStringSerializerHelper.MAP_BEGIN);

			int items = 0;
			if (iLinkedClass != null) {
				// EMBEDDED OBJECTS
				ODocument record;
				for (Entry<String, Object> o : ((Map<String, Object>) iValue).entrySet()) {
					if (items > 0)
						buffer.append(OStringSerializerHelper.RECORD_SEPARATOR);

					if (o != null) {
						if (o instanceof ODocument)
							record = (ODocument) o;
						else
							record = OObjectSerializerHelper.toStream(o, new ODocument(o.getClass().getSimpleName()),
									iDatabase instanceof ODatabaseObjectTx ? ((ODatabaseObjectTx) iDatabase).getEntityManager()
											: OEntityManagerInternal.INSTANCE, iLinkedClass, iObjHandler != null ? iObjHandler
											: new OUserObject2RecordHandler() {

												public Object getUserObjectByRecord(ORecordInternal<?> iRecord) {
													return iRecord;
												}

												public ORecordInternal<?> getRecordByUserObject(Object iPojo, boolean iIsMandatory) {
													return new ODocument(iLinkedClass);
												}
											});

						buffer.append(toString(record, null, iObjHandler, iMarshalledRecords));
					}

					items++;
				}
			} else
				// EMBEDDED LITERALS
				for (Entry<String, Object> o : ((Map<String, Object>) iValue).entrySet()) {
					if (items > 0)
						buffer.append(OStringSerializerHelper.RECORD_SEPARATOR);

					buffer.append(OStringSerializerHelper.fieldTypeToString(OType.STRING, o.getKey()));
					buffer.append(OStringSerializerHelper.ENTRY_SEPARATOR);
					buffer.append(OStringSerializerHelper.fieldTypeToString(iLinkedType, o.getValue()));
					items++;
				}

			buffer.append(OStringSerializerHelper.MAP_END);
			break;
		}

		case LINKLIST:
		case LINKSET: {
			buffer.append(OStringSerializerHelper.COLLECTION_BEGIN);

			int items = 0;
			// LINKED OBJECTS
			for (ORecordSchemaAware<?> record : (Collection<ORecordSchemaAware<?>>) iValue) {
				if (items++ > 0)
					buffer.append(OStringSerializerHelper.RECORD_SEPARATOR);

				// ASSURE THE OBJECT IS SAVED
				if (record.getDatabase() == null)
					record.setDatabase(iDatabase);

				linkToStream(buffer, iRecord, record);
			}

			buffer.append(OStringSerializerHelper.COLLECTION_END);
			break;
		}

		default:
			return OStringSerializerHelper.fieldTypeToString(iType, iValue);
		}

		return buffer.toString();
	}

	public static OType getNumber(final DecimalFormatSymbols unusualSymbols, final String value) {
		boolean integer = true;
		char c;

		for (int index = 0; index < value.length(); ++index) {
			c = value.charAt(index);
			if (c < '0' || c > '9')
				if ((index == 0 && (c == '+' || c == '-')))
					continue;
				else if (c == unusualSymbols.getDecimalSeparator())
					integer = false;
				else {
					return OType.STRING;
				}
		}

		return integer ? OType.INTEGER : OType.FLOAT;
	}

	private void linkToStream(final StringBuilder buffer, final ORecordSchemaAware<?> iParentRecord,
			final ORecordSchemaAware<?> iLinkedRecord) {

		ORID link = iLinkedRecord.getIdentity();
		if (!link.isValid()) {
			// OVERWRITE THE DATABASE TO THE SAME OF PARENT ONE
			iLinkedRecord.setDatabase(iParentRecord.getDatabase());

			// STORE THE TRAVERSED OBJECT TO KNOW THE RECORD ID. CALL THIS VERSION TO AVOID CLEAR OF STACK IN THREAD-LOCAL
			iLinkedRecord.getDatabase().save((ORecordInternal) iLinkedRecord);
		}

		buffer.append(OStringSerializerHelper.LINK);

		if (iLinkedRecord.getClassName() != null) {
			buffer.append(iLinkedRecord.getClassName());
			buffer.append(OStringSerializerHelper.CLASS_SEPARATOR);
		}

		buffer.append(link.toString());
	}
}

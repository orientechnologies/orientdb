/*
 *
 * Copyright 2012 Luca Molino (molino.luca--AT--gmail.com)
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
package com.orientechnologies.orient.core.fetch.object;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.db.object.OLazyObjectList;
import com.orientechnologies.orient.core.db.object.OLazyObjectMap;
import com.orientechnologies.orient.core.db.object.OLazyObjectSet;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMap;
import com.orientechnologies.orient.core.db.record.ORecordLazySet;
import com.orientechnologies.orient.core.exception.OFetchException;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.fetch.OFetchListener;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.object.OObjectSerializerHelper;

/**
 * @author luca.molino
 * 
 */
public class OObjectFetchListener implements OFetchListener {

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void processStandardField(final ORecordSchemaAware<?> iRecord, final Object iFieldValue, final String iFieldName, final OFetchContext iContext, final Object iUserObject)
			throws OFetchException {
		if (iFieldValue instanceof ORecordLazyList)
			OObjectSerializerHelper.setFieldValue(iUserObject, iFieldName, new OLazyObjectList(iRecord, (ORecordLazyList) iFieldValue));
		else if (iFieldValue instanceof ORecordLazySet)
			OObjectSerializerHelper.setFieldValue(iUserObject, iFieldName, new OLazyObjectSet(iRecord, (ORecordLazyList) iFieldValue));
		else if (iFieldValue instanceof ORecordLazyMap)
			OObjectSerializerHelper.setFieldValue(iUserObject, iFieldName, new OLazyObjectMap(iRecord, (ORecordLazyMap) iFieldValue));
		else
			OObjectSerializerHelper.setFieldValue(iUserObject, iFieldName, iFieldValue);
	}

	public void processStandardCollectionValue(Object iFieldValue, OFetchContext iContext) throws OFetchException {
	}

	public void parseLinked(final ORecordSchemaAware<?> iRootRecord, final OIdentifiable iLinked, final Object iUserObject, final String iFieldName, final OFetchContext iContext)
			throws OFetchException {
		final Class<?> type = OObjectSerializerHelper.getFieldType(iUserObject, iFieldName);
		if (type == null || Map.class.isAssignableFrom(type)) {
		} else if (Set.class.isAssignableFrom(type) || Collection.class.isAssignableFrom(type) || type.isArray()) {
			if (!((OObjectFetchContext) iContext).isLazyLoading()) {
				Object value = ((OObjectFetchContext) iContext).getObj2RecHandler().getUserObjectByRecord((ODocument) iLinked, ((OObjectFetchContext) iContext).getFetchPlan());
				if (!((OObjectFetchContext) iContext).isLazyLoading()) {
					Collection<Object> target = (Collection<Object>) OObjectSerializerHelper.getFieldValue(iUserObject, iFieldName);
					target.add(value);
					OObjectSerializerHelper.setFieldValue(iUserObject, iFieldName, target);
				}
			}
			return;
		} else if (iLinked instanceof ORecordSchemaAware && !(((OObjectFetchContext) iContext).getObj2RecHandler().existsUserObjectByRID(iLinked.getIdentity()))) {
			fetchLinked(iRootRecord, iUserObject, iFieldName, (ORecordSchemaAware<?>) iLinked, iContext);
		}
	}

	public Object fetchLinkedMapEntry(final ORecordSchemaAware<?> iRoot, final Object iUserObject, final String iFieldName, String iKey, final ORecordSchemaAware<?> iLinked,
			final OFetchContext iContext) throws OFetchException {
		Object value = null;
		final Class<?> type = OObjectSerializerHelper.getFieldType((ODocument) iLinked, ((OObjectFetchContext) iContext).getEntityManager());
		final Class<?> fieldClass = ((OObjectFetchContext) iContext).getEntityManager().getEntityClass(type.getSimpleName());
		if (fieldClass != null) {
			// RECOGNIZED TYPE
			value = ((OObjectFetchContext) iContext).getObj2RecHandler().getUserObjectByRecord((ODocument) iLinked, ((OObjectFetchContext) iContext).getFetchPlan());
		}
		return value;
	}

	@SuppressWarnings("unchecked")
	public Object fetchLinkedCollectionValue(final ORecordSchemaAware<?> iRoot, final Object iUserObject, final String iFieldName, final ORecordSchemaAware<?> iLinked,
			final OFetchContext iContext) throws OFetchException {
		Object value = null;
		final Class<?> fieldClass = OObjectSerializerHelper.getFieldType((ODocument) iLinked, ((OObjectFetchContext) iContext).getEntityManager());

		if (fieldClass != null) {
			// RECOGNIZED TYPE
			value = ((OObjectFetchContext) iContext).getObj2RecHandler().getUserObjectByRecord((ODocument) iLinked, ((OObjectFetchContext) iContext).getFetchPlan());
			if (!((OObjectFetchContext) iContext).isLazyLoading()) {
				Collection<Object> target = (Collection<Object>) OObjectSerializerHelper.getFieldValue(iUserObject, iFieldName);
				target.add(value);
				OObjectSerializerHelper.setFieldValue(iUserObject, iFieldName, target);
			}
		}
		return value;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Object fetchLinked(ORecordSchemaAware<?> iRoot, Object iUserObject, String iFieldName, ORecordSchemaAware<?> iLinked, OFetchContext iContext) throws OFetchException {
		if (iUserObject == null)
			return null;
		final Class<?> type;
		if (iLinked != null && iLinked instanceof ODocument)
			// GET TYPE BY DOCUMENT'S CLASS. THIS WORKS VERY WELL FOR SUB-TYPES
			type = OObjectSerializerHelper.getFieldType((ODocument) iLinked, ((OObjectFetchContext) iContext).getEntityManager());
		else
			// DETERMINE TYPE BY REFLECTION
			type = OObjectSerializerHelper.getFieldType(iUserObject, iFieldName);

		if (type == null)
			throw new OSerializationException(
					"Linked type of field '"
							+ iRoot.getClassName()
							+ "."
							+ iFieldName
							+ "' is unknown. Probably needs to be registered with <db>.getEntityManager().registerEntityClasses(<package>) or <db>.getEntityManager().registerEntityClass(<class>) or the package cannot be loaded correctly due to a classpath problem. In this case register the single classes one by one.");

		Object fieldValue = null;
		Class<?> fieldClass;
		if (type.isEnum()) {

			String enumName = ((ODocument) iLinked).field(iFieldName);
			Class<Enum> enumClass = (Class<Enum>) type;
			fieldValue = Enum.valueOf(enumClass, enumName);

		} else {

			fieldClass = ((OObjectFetchContext) iContext).getEntityManager().getEntityClass(type.getSimpleName());
			if (fieldClass != null) {
				// RECOGNIZED TYPE
				fieldValue = ((OObjectFetchContext) iContext).getObj2RecHandler().getUserObjectByRecord((ODocument) iLinked, ((OObjectFetchContext) iContext).getFetchPlan());
			}
		}

		OObjectSerializerHelper.setFieldValue(iUserObject, iFieldName,
				OObjectSerializerHelper.unserializeFieldValue(OObjectSerializerHelper.getFieldType(iUserObject, iFieldName), fieldValue));

		return fieldValue;
	}
}

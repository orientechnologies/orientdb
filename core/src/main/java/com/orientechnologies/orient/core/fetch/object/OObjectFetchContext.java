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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.db.OUserObject2RecordHandler;
import com.orientechnologies.orient.core.db.object.OLazyObjectList;
import com.orientechnologies.orient.core.db.object.OLazyObjectMap;
import com.orientechnologies.orient.core.db.object.OLazyObjectSet;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.entity.OEntityManager;
import com.orientechnologies.orient.core.exception.OFetchException;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.serialization.serializer.object.OObjectSerializerHelper;

/**
 * @author luca.molino
 * 
 */
public class OObjectFetchContext implements OFetchContext {

	protected final String										fetchPlan;
	protected final boolean										lazyLoading;
	protected final OEntityManager						entityManager;
	protected final OUserObject2RecordHandler	obj2RecHandler;

	public OObjectFetchContext(final String iFetchPlan, final boolean iLazyLoading, final OEntityManager iEntityManager,
			final OUserObject2RecordHandler iObj2RecHandler) {
		fetchPlan = iFetchPlan;
		lazyLoading = iLazyLoading;
		obj2RecHandler = iObj2RecHandler;
		entityManager = iEntityManager;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void onBeforeMap(ORecordSchemaAware<?> iRootRecord, String iFieldName, final Object iUserObject) throws OFetchException {
		final Map<Object, Object> map = (Map<Object, Object>) iRootRecord.field(iFieldName);
		final Map<Object, Object> target;
		if (lazyLoading)
			target = new OLazyObjectMap<Object>(iRootRecord, map).setFetchPlan(fetchPlan);
		else {
			target = new HashMap();
		}
		OObjectSerializerHelper.setFieldValue(iUserObject, iFieldName, target);
	}

	public void onBeforeArray(ORecordSchemaAware<?> iRootRecord, String iFieldName, Object iUserObject, OIdentifiable[] iArray)
			throws OFetchException {
		OObjectSerializerHelper.setFieldValue(iUserObject, iFieldName,
				Array.newInstance(iRootRecord.getSchemaClass().getProperty(iFieldName).getLinkedClass().getJavaClass(), iArray.length));
	}

	public void onAfterArray(final ORecordSchemaAware<?> iRootRecord, final String iFieldName, Object iUserObject)
			throws OFetchException {
	}

	public void onAfterMap(final ORecordSchemaAware<?> iRootRecord, final String iFieldName, final Object iUserObject)
			throws OFetchException {
	}

	public void onBeforeDocument(final ORecordSchemaAware<?> iRecord, final ORecordSchemaAware<?> iDocument, String iFieldName,
			final Object iUserObject) throws OFetchException {
	}

	public void onAfterDocument(final ORecordSchemaAware<?> iRootRecord, final ORecordSchemaAware<?> iDocument, String iFieldName,
			final Object iUserObject) throws OFetchException {
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void onBeforeCollection(ORecordSchemaAware<?> iRootRecord, String iFieldName, final Object iUserObject,
			final Collection<?> iCollection) throws OFetchException {
		final Class<?> type = OObjectSerializerHelper.getFieldType(iUserObject, iFieldName);
		final Collection target;
		if (type != null && Set.class.isAssignableFrom(type)) {
			if (lazyLoading)
				target = new OLazyObjectSet<Object>(iRootRecord, (Collection<Object>) iCollection).setFetchPlan(fetchPlan);
			else {
				target = new HashSet();
			}
		} else {
			final Collection<Object> list = (Collection<Object>) iCollection;
			if (lazyLoading)
				target = new OLazyObjectList<Object>(iRootRecord, list).setFetchPlan(fetchPlan);
			else {
				target = new ArrayList();
			}
		}
		OObjectSerializerHelper.setFieldValue(iUserObject, iFieldName, target);
	}

	public void onAfterCollection(ORecordSchemaAware<?> iRootRecord, String iFieldName, final Object iUserObject)
			throws OFetchException {
	}

	public void onAfterFetch(ORecordSchemaAware<?> iRootRecord) throws OFetchException {
	}

	public void onBeforeFetch(ORecordSchemaAware<?> iRootRecord) throws OFetchException {
	}

	public void onBeforeStandardField(Object iFieldValue, String iFieldName, Object iUserObject) {
	}

	public void onAfterStandardField(Object iFieldValue, String iFieldName, Object iUserObject) {
	}

	public OUserObject2RecordHandler getObj2RecHandler() {
		return obj2RecHandler;
	}

	public OEntityManager getEntityManager() {
		return entityManager;
	}

	public boolean isLazyLoading() {
		return lazyLoading;
	}

	public String getFetchPlan() {
		return fetchPlan;
	}

	public boolean fetchEmbeddedDocuments() {
		return true;
	}
}

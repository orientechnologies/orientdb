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
package com.orientechnologies.orient.core.fetch;

import java.util.Collection;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OFetchException;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;

/**
 * @author luca.molino
 * 
 */
public interface OFetchContext {

	public void onBeforeFetch(final ORecordSchemaAware<?> iRootRecord) throws OFetchException;

	public void onAfterFetch(final ORecordSchemaAware<?> iRootRecord) throws OFetchException;

	public void onBeforeArray(final ORecordSchemaAware<?> iRootRecord, final String iFieldName, final Object iUserObject,
			final OIdentifiable[] iArray) throws OFetchException;

	public void onAfterArray(final ORecordSchemaAware<?> iRootRecord, final String iFieldName, final Object iUserObject)
			throws OFetchException;

	public void onBeforeCollection(final ORecordSchemaAware<?> iRootRecord, final String iFieldName, final Object iUserObject,
			final Collection<?> iCollection) throws OFetchException;

	public void onAfterCollection(final ORecordSchemaAware<?> iRootRecord, final String iFieldName, final Object iUserObject)
			throws OFetchException;

	public void onBeforeMap(final ORecordSchemaAware<?> iRootRecord, final String iFieldName, final Object iUserObject)
			throws OFetchException;

	public void onAfterMap(final ORecordSchemaAware<?> iRootRecord, final String iFieldName, final Object iUserObject)
			throws OFetchException;

	public void onBeforeDocument(final ORecordSchemaAware<?> iRecord, final ORecordSchemaAware<?> iDocument, final String iFieldName,
			final Object iUserObject) throws OFetchException;

	public void onAfterDocument(final ORecordSchemaAware<?> iRootRecord, final ORecordSchemaAware<?> iDocument,
			final String iFieldName, final Object iUserObject) throws OFetchException;

	public void onBeforeStandardField(final Object iFieldValue, final String iFieldName, final Object iUserObject);

	public void onAfterStandardField(final Object iFieldValue, final String iFieldName, final Object iUserObject);

	public boolean fetchEmbeddedDocuments();
}

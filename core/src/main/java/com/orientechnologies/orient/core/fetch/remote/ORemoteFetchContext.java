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
package com.orientechnologies.orient.core.fetch.remote;

import java.util.Collection;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OFetchException;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;

/**
 * Fetch context for {@class ONetworkBinaryProtocol} class
 * 
 * @author luca.molino
 * 
 */
public class ORemoteFetchContext implements OFetchContext {
	public void onBeforeStandardField(Object iFieldValue, String iFieldName, Object iUserObject) {
	}

	public void onAfterStandardField(Object iFieldValue, String iFieldName, Object iUserObject) {
	}

	public void onBeforeMap(ORecordSchemaAware<?> iRootRecord, String iFieldName, final Object iUserObject) throws OFetchException {
	}

	public void onBeforeFetch(ORecordSchemaAware<?> iRootRecord) throws OFetchException {
	}

	public void onBeforeArray(ORecordSchemaAware<?> iRootRecord, String iFieldName, Object iUserObject, OIdentifiable[] iArray)
			throws OFetchException {
	}

	public void onAfterArray(ORecordSchemaAware<?> iRootRecord, String iFieldName, Object iUserObject) throws OFetchException {
	}

	public void onBeforeDocument(ORecordSchemaAware<?> iRecord, String iFieldName, final Object iUserObject) throws OFetchException {
	}

	public void onBeforeCollection(ORecordSchemaAware<?> iRootRecord, String iFieldName, final Object iUserObject,
			final Collection<?> iCollection) throws OFetchException {
	}

	public void onAfterMap(ORecordSchemaAware<?> iRootRecord, String iFieldName, final Object iUserObject) throws OFetchException {
	}

	public void onAfterFetch(ORecordSchemaAware<?> iRootRecord) throws OFetchException {
	}

	public void onAfterDocument(ORecordSchemaAware<?> iRootRecord, String iFieldName, final Object iUserObject)
			throws OFetchException {
	}

	public void onAfterCollection(ORecordSchemaAware<?> iRootRecord, String iFieldName, final Object iUserObject)
			throws OFetchException {
	}

	public boolean fetchEmbeddedDocuments() {
		return false;
	}
}

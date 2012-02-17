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
package com.orientechnologies.orient.core.fetch.json;

import java.io.IOException;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OFetchException;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.fetch.OFetchListener;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;

/**
 * @author luca.molino
 * 
 */
public class OJSONFetchListener implements OFetchListener {

	public void processStandardField(final ORecordSchemaAware<?> iRecord, final Object iFieldValue, final String iFieldName, final OFetchContext iContext, final Object iusObject) {
		try {
			((OJSONFetchContext) iContext).getJsonWriter().writeAttribute(((OJSONFetchContext) iContext).getIndentLevel(), true, iFieldName, OJSONWriter.encode(iFieldValue));
		} catch (IOException e) {
			throw new OFetchException("Error processing field '" + iFieldValue + " of record " + iRecord.getIdentity(), e);
		}
	}

	public void processStandardCollectionValue(final Object iFieldValue, final OFetchContext iContext) throws OFetchException {
		try {
			((OJSONFetchContext) iContext).getJsonWriter().writeValue(((OJSONFetchContext) iContext).getIndentLevel(), true, OJSONWriter.encode(iFieldValue));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Object fetchLinked(final ORecordSchemaAware<?> iRecord, final Object iUserObject, final String iFieldName, final ORecordSchemaAware<?> iLinked,
			final OFetchContext iContext) throws OFetchException {
		return iLinked;
	}

	public Object fetchLinkedMapEntry(final ORecordSchemaAware<?> iRecord, final Object iUserObject, final String iFieldName, final String iKey, final ORecordSchemaAware<?> iLinked,
			final OFetchContext iContext) throws OFetchException {
		return iLinked;
	}

	public void parseLinked(final ORecordSchemaAware<?> iRootRecord, final OIdentifiable iLinked, final Object iUserObject, final String iFieldName, final OFetchContext iContext)
			throws OFetchException {
		try {
			((OJSONFetchContext) iContext).writeLinkedValue(iLinked, iFieldName);
		} catch (IOException e) {
			throw new OFetchException("Error writing linked field " + iFieldName + " (record:" + iLinked.getIdentity() + ") of record " + iRootRecord.getIdentity(), e);
		}
	}

	public Object fetchLinkedCollectionValue(ORecordSchemaAware<?> iRoot, Object iUserObject, String iFieldName, ORecordSchemaAware<?> iLinked, OFetchContext iContext)
			throws OFetchException {
		return iLinked;
	}

}

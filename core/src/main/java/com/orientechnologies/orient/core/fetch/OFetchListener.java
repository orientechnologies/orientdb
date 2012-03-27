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
package com.orientechnologies.orient.core.fetch;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OFetchException;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;

/**
 * Listener interface used while fetching records.
 * 
 * @author Luca Garulli
 * 
 */
public interface OFetchListener {
	/**
	 * Fetch the linked field.
	 * 
	 * @param iRoot
	 * @param iFieldName
	 * @param iLinked
	 * @return null if the fetching must stop, otherwise the current field value
	 */
	public Object fetchLinked(final ORecordSchemaAware<?> iRoot, final Object iUserObject, final String iFieldName, final ORecordSchemaAware<?> iLinked, final OFetchContext iContext)
			throws OFetchException;

	public void parseLinked(final ORecordSchemaAware<?> iRootRecord, final OIdentifiable iLinked, final Object iUserObject, final String iFieldName, final OFetchContext iContext)
			throws OFetchException;
	
	public void parseLinkedCollectionValue(final ORecordSchemaAware<?> iRootRecord, final OIdentifiable iLinked, final Object iUserObject, final String iFieldName, final OFetchContext iContext)
	throws OFetchException;

	public Object fetchLinkedMapEntry(final ORecordSchemaAware<?> iRoot, final Object iUserObject, final String iFieldName, final String iKey, final ORecordSchemaAware<?> iLinked,
			final OFetchContext iContext) throws OFetchException;

	public Object fetchLinkedCollectionValue(final ORecordSchemaAware<?> iRoot, final Object iUserObject, final String iFieldName, final ORecordSchemaAware<?> iLinked,
			final OFetchContext iContext) throws OFetchException;

	public void processStandardField(final ORecordSchemaAware<?> iRecord, final Object iFieldValue, final String iFieldName, final OFetchContext iContext, final Object iUserObject)
			throws OFetchException;

}

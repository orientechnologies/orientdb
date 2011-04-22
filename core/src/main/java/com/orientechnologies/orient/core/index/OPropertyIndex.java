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
package com.orientechnologies.orient.core.index;

import java.util.Arrays;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Index implementation bound to one or more schema class properties.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OPropertyIndex implements OIndexCallback {
	protected OIndex						delegate;
	protected String[]					fields;
	protected static final char	FIELD_SEPARATOR	= '+';

	public OPropertyIndex(final ODatabaseRecord iDatabase, final OClass iClass, final String[] iFields, final String iType,
			final OProgressListener iProgressListener) {
		fields = iFields;
		final String indexName = getIndexName(iClass, iFields);

		delegate = iDatabase.getMetadata().getIndexManager()
				.createIndex(indexName, iType, iClass.getClusterIds(), this, iProgressListener, true);
	}

	public OPropertyIndex(final OIndex iIndex, final String[] iFields) {
		fields = iFields;
		delegate = iIndex;
	}

	public OPropertyIndex(final ODatabaseRecord iDatabase, final OClass iClass, final String[] iFields, final ODocument iConfiguration) {
		fields = iFields;
		final String indexName = getIndexName(iClass, iFields);

		delegate = iDatabase.getMetadata().getIndexManager().loadIndex(indexName, iConfiguration);
		if (delegate != null)
			delegate.setCallback(this);
	}

	public void checkEntry(final ODocument iRecord) {
		// GENERATE THE KEY
		final Object key = generateKey(iRecord);

		try {
			delegate.checkEntry(iRecord, key);
		} catch (OIndexException e) {
			OLogManager.instance().exception("Invalid constraints on index '%s' for key '%s' in record %s for the fields '%s'", e,
					OIndexException.class, delegate.getName(), key, iRecord.getIdentity(), Arrays.toString(fields));
		}
	}

	public void setCallback(OIndexCallback iCallback) {
	}

	public OIndex getUnderlying() {
		return delegate;
	}

	public Object getDocumentValueToIndex(ODocument iDocument) {
		return generateKey(iDocument);
	}

	private Object generateKey(final ODocument iRecord) {
		if (fields.length == 1)
			// ONE-FIELD KEY
			return iRecord.field(fields[0]);

		// MULTI KEY USED IN COMPOSED PROPERTY INDEXES
		StringBuilder buffer = new StringBuilder();
		for (String f : fields) {
			if (buffer.length() > 0)
				buffer.append(FIELD_SEPARATOR);

			if (f == null)
				buffer.append('-');
			else
				buffer.append(iRecord.field(f));
		}
		return buffer.toString();
	}

	@Override
	public String toString() {
		return delegate != null ? delegate.toString() : "no-index";
	}

	private String getIndexName(final OClass iClass, final String[] iFields) {
		final StringBuilder indexName = new StringBuilder();
		indexName.append(iClass.getName());
		indexName.append('.');

		boolean first = true;
		for (String f : iFields) {
			if (first)
				first = false;
			else
				indexName.append('_');

			indexName.append(f);
		}
		return indexName.toString();
	}
}

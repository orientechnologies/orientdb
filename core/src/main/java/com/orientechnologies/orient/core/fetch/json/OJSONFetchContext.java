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
import java.util.Collection;
import java.util.Date;
import java.util.Stack;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OFetchException;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordSchemaAware;
import com.orientechnologies.orient.core.record.impl.ODocumentHelper;
import com.orientechnologies.orient.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerJSON;

/**
 * @author luca.molino
 * 
 */
public class OJSONFetchContext implements OFetchContext {

	protected final OJSONWriter										jsonWriter;
	protected int																	indentLevel			= 0;
	protected final boolean												includeType;
	protected final boolean												includeId;
	protected final boolean												includeVer;
	protected final boolean												includeClazz;
	protected final boolean												attribSameRow;
	protected final boolean												alwaysFetchEmbeddedDocuments;
	protected final boolean												keepTypes;
	protected final Stack<StringBuilder>					typesStack			= new Stack<StringBuilder>();
	protected final Stack<ORecordSchemaAware<?>>	collectionStack	= new Stack<ORecordSchemaAware<?>>();

	public OJSONFetchContext(final OJSONWriter iJsonWriter, boolean iIncludeType, final boolean iIncludeId,
			final boolean iIncludeVer, final boolean iIncludeClazz, final boolean iAttribSameRow, final boolean iKeepTypes,
			final boolean iAlwaysFetchEmbeddedDocuments) {
		jsonWriter = iJsonWriter;
		includeType = iIncludeType;
		includeClazz = iIncludeClazz;
		includeId = iIncludeId;
		includeVer = iIncludeVer;
		attribSameRow = iAttribSameRow;
		keepTypes = iKeepTypes;
		alwaysFetchEmbeddedDocuments = iAlwaysFetchEmbeddedDocuments;
	}

	public void onBeforeFetch(final ORecordSchemaAware<?> iRootRecord) {
		typesStack.add(new StringBuilder());
	}

	public void onAfterFetch(final ORecordSchemaAware<?> iRootRecord) {
		StringBuilder buffer = typesStack.pop();
		if (keepTypes && buffer.length() > 0)
			try {
				jsonWriter.writeAttribute(indentLevel + 1, true, ORecordSerializerJSON.ATTRIBUTE_FIELD_TYPES, buffer.toString());
			} catch (IOException e) {
				throw new OFetchException("Error writing field types", e);
			}
	}

	public void onBeforeStandardField(final Object iFieldValue, final String iFieldName, final Object iUserObject) {
		if (keepTypes) {
			// StringBuilder buffer = typesStack.pop();
			if (iFieldValue instanceof Long)
				appendType(typesStack.peek(), iFieldName, 'l');
			else if (iFieldValue instanceof Float)
				appendType(typesStack.peek(), iFieldName, 'f');
			else if (iFieldValue instanceof Short)
				appendType(typesStack.peek(), iFieldName, 's');
			else if (iFieldValue instanceof Double)
				appendType(typesStack.peek(), iFieldName, 'd');
			else if (iFieldValue instanceof Date)
				appendType(typesStack.peek(), iFieldName, 't');
			else if (iFieldValue instanceof Byte || iFieldValue instanceof byte[])
				appendType(typesStack.peek(), iFieldName, 'b');
			// typesStack.add(buffer);
		}
	}

	public void onAfterStandardField(Object iFieldValue, String iFieldName, Object iUserObject) {
	}

	public void onBeforeArray(final ORecordSchemaAware<?> iRootRecord, final String iFieldName, final Object iUserObject,
			final OIdentifiable[] iArray) {
		onBeforeCollection(iRootRecord, iFieldName, iUserObject, null);
	}

	public void onAfterArray(final ORecordSchemaAware<?> iRootRecord, final String iFieldName, final Object iUserObject) {
		onAfterCollection(iRootRecord, iFieldName, iUserObject);
	}

	public void onBeforeCollection(final ORecordSchemaAware<?> iRootRecord, final String iFieldName, final Object iUserObject,
			final Collection<?> iCollection) {
		indentLevel++;
		try {
			jsonWriter.beginCollection(indentLevel, true, iFieldName);
			collectionStack.add(iRootRecord);
		} catch (IOException e) {
			throw new OFetchException("Error writing collection field " + iFieldName + " of record " + iRootRecord.getIdentity(), e);
		}
	}

	public void onAfterCollection(final ORecordSchemaAware<?> iRootRecord, final String iFieldName, final Object iUserObject) {
		try {
			jsonWriter.endCollection(indentLevel, false);
			collectionStack.pop();
		} catch (IOException e) {
			throw new OFetchException("Error writing collection field " + iFieldName + " of record " + iRootRecord.getIdentity(), e);
		}
		indentLevel--;
	}

	public void onBeforeMap(final ORecordSchemaAware<?> iRootRecord, final String iFieldName, final Object iUserObject) {
		indentLevel++;
		try {
			jsonWriter.beginObject(indentLevel, true, iFieldName);
		} catch (IOException e) {
			throw new OFetchException("Error writing map field " + iFieldName + " of record " + iRootRecord.getIdentity(), e);
		}
	}

	public void onAfterMap(final ORecordSchemaAware<?> iRootRecord, final String iFieldName, final Object iUserObject) {
		try {
			jsonWriter.endObject(indentLevel, true);
		} catch (IOException e) {
			throw new OFetchException("Error writing map field " + iFieldName + " of record " + iRootRecord.getIdentity(), e);
		}
		indentLevel--;
	}

	public void onBeforeDocument(final ORecordSchemaAware<?> iRootRecord, final ORecordSchemaAware<?> iDocument,
			final String iFieldName, final Object iUserObject) {
		indentLevel++;
		try {
			final String fieldName;
			if (!collectionStack.isEmpty() && collectionStack.peek().equals(iRootRecord))
				fieldName = null;
			else
				fieldName = iFieldName;
			jsonWriter.beginObject(indentLevel, true, fieldName);
			writeSignature(jsonWriter, indentLevel, includeType, includeId, includeVer, includeClazz, attribSameRow, iDocument);
		} catch (IOException e) {
			throw new OFetchException("Error writing link field " + iFieldName + " of record " + iRootRecord.getIdentity(), e);
		}
	}

	public void onAfterDocument(final ORecordSchemaAware<?> iRootRecord, final ORecordSchemaAware<?> iDocument,
			final String iFieldName, final Object iUserObject) {
		try {
			jsonWriter.endObject(indentLevel + 1, true);
		} catch (IOException e) {
			throw new OFetchException("Error writing link field " + iFieldName + " of record " + iRootRecord.getIdentity(), e);
		}
		indentLevel--;
	}

	public void writeLinkedValue(final OIdentifiable iRecord, final String iFieldName) throws IOException {
		jsonWriter.writeValue(indentLevel, true, OJSONWriter.encode(iRecord.getIdentity()));
	}

	public void writeLinkedAttribute(final OIdentifiable iRecord, final String iFieldName) throws IOException {
		jsonWriter.writeAttribute(indentLevel, true, iFieldName, OJSONWriter.encode(iRecord.getIdentity()));
	}

	public boolean isInCollection(ORecordSchemaAware<?> record) {
		return !collectionStack.isEmpty() && collectionStack.peek().equals(record);
	}

	public OJSONWriter getJsonWriter() {
		return jsonWriter;
	}

	public int getIndentLevel() {
		return indentLevel;
	}

	private void appendType(final StringBuilder iBuffer, final String iFieldName, final char iType) {
		if (iBuffer.length() > 0)
			iBuffer.append(',');
		iBuffer.append(iFieldName);
		iBuffer.append('=');
		iBuffer.append(iType);
	}

	public void writeSignature(final OJSONWriter json, int indentLevel, boolean includeType, boolean includeId, boolean includeVer,
			boolean includeClazz, boolean attribSameRow, final ORecordInternal<?> record) throws IOException {
		boolean firstAttribute = true;
		if (includeType) {
			json.writeAttribute(firstAttribute ? indentLevel + 1 : 0, firstAttribute, ODocumentHelper.ATTRIBUTE_TYPE,
					"" + (char) record.getRecordType());
			if (attribSameRow)
				firstAttribute = false;
		}
		if (includeId && record.getIdentity() != null && record.getIdentity().isValid()) {
			json.writeAttribute(!firstAttribute ? indentLevel + 1 : 0, firstAttribute, ODocumentHelper.ATTRIBUTE_RID, record
					.getIdentity().toString());
			if (attribSameRow)
				firstAttribute = false;
		}
		if (includeVer) {
			json.writeAttribute(firstAttribute ? indentLevel + 1 : 0, firstAttribute, ODocumentHelper.ATTRIBUTE_VERSION,
					record.getVersion());
			if (attribSameRow)
				firstAttribute = false;
		}
		if (includeClazz && record instanceof ORecordSchemaAware<?> && ((ORecordSchemaAware<?>) record).getClassName() != null) {
			json.writeAttribute(firstAttribute ? indentLevel + 1 : 0, firstAttribute, ODocumentHelper.ATTRIBUTE_CLASS,
					((ORecordSchemaAware<?>) record).getClassName());
			if (attribSameRow)
				firstAttribute = false;
		}
	}

	public boolean fetchEmbeddedDocuments() {
		return alwaysFetchEmbeddedDocuments;
	}
}

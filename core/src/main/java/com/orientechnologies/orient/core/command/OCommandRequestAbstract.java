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
package com.orientechnologies.orient.core.command;

import java.io.IOException;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.OSerializableStream;

/**
 * Text based Command Request abstract class.
 * 
 * @author Luca Garulli
 * 
 */
public abstract class OCommandRequestAbstract implements OCommandRequestInternal {
	protected String									text;
	protected ODatabaseRecord<?>			database;
	protected int											limit	= -1;
	protected OCommandResultListener	resultListener;
	protected Object[]								parameters;

	protected OCommandRequestAbstract() {
	}

	protected OCommandRequestAbstract(final String iText) {
		this(iText, null);
	}

	protected OCommandRequestAbstract(final String iText, final ODatabaseRecord<ODocument> iDatabase) {
		text = iText;
		database = iDatabase;
	}

	/**
	 * Delegates the execution to the configured command executor.
	 */
	@SuppressWarnings("unchecked")
	public <RET> RET execute(final Object... iArgs) {
		parameters = iArgs;
		return (RET) database.getStorage().command(this);
	}

	public String getText() {
		return text;
	}

	public OSerializableStream fromStream(byte[] iStream) throws IOException {
		text = OBinaryProtocol.bytes2string(iStream);
		return this;
	}

	public byte[] toStream() throws IOException {
		return OBinaryProtocol.string2bytes(text);
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + " [text=" + text + "]";
	}

	public ODatabaseRecord<?> getDatabase() {
		return database;
	}

	public OCommandRequestInternal setDatabase(final ODatabaseRecord<?> iDatabase) {
		this.database = iDatabase;
		return this;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public OCommandResultListener getResultListener() {
		return resultListener;
	}

	public void setResultListener(OCommandResultListener iListener) {
		resultListener = iListener;
	}

	public Object[] getParameters() {
		return parameters;
	}
}

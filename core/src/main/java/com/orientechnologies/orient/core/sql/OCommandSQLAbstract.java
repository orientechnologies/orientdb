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
package com.orientechnologies.orient.core.sql;

import java.io.IOException;

import com.orientechnologies.orient.core.command.OCommandInternal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.OSerializableStream;

/**
 * SQL command implementation.
 * 
 * @author luca
 * 
 */
public abstract class OCommandSQLAbstract implements OCommandInternal, OSerializableStream {
	protected String							text;
	protected String							textUpperCase;
	protected ODatabaseRecord<?>	database;

	public OCommandSQLAbstract(final String iText, final String iTextUpperCase) {
		this(iText, iTextUpperCase, null);
	}

	public OCommandSQLAbstract(final String iText, final String iTextUpperCase, final ODatabaseRecord<?> iDatabase) {
		text = iText;
		textUpperCase = iTextUpperCase;
		database = iDatabase;
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

	public void setDatabase(ODatabaseRecord<?> database) {
		this.database = database;
	}
}

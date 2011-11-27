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
package com.orientechnologies.orient.core.serialization.serializer.stream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringSerializerAnyRuntime;

/**
 * Uses the Java serialization.
 * 
 * @see OStringSerializerAnyRuntime
 * @author Luca Garulli
 * 
 */
public class OStreamSerializerAnyRuntime implements OStreamSerializer {
	private static final String											NAME			= "au";
	public static final OStreamSerializerAnyRuntime	INSTANCE	= new OStreamSerializerAnyRuntime();

	public String getName() {
		return NAME;
	}

	/**
	 * Re-Create any object if the class has a public constructor that accepts a String as unique parameter.
	 */
	public Object fromStream(final ODatabaseRecord iDatabase, final byte[] iStream) throws IOException {
		if (iStream == null || iStream.length == 0)
			// NULL VALUE
			return null;

		final ByteArrayInputStream is = new ByteArrayInputStream(iStream);
		final ObjectInputStream in = new ObjectInputStream(is);
		try {
			return in.readObject();
		} catch (ClassNotFoundException e) {
			throw new OSerializationException("Cannot unmarshall Java serialized object", e);
		} finally {
			in.close();
			is.close();
		}
	}

	public byte[] toStream(final ODatabaseRecord iDatabase, final Object iObject) throws IOException {
		if (iObject == null)
			return new byte[0];

		final ByteArrayOutputStream os = new ByteArrayOutputStream();
		final ObjectOutputStream oos = new ObjectOutputStream(os);
		oos.writeObject(iObject);
		oos.close();

		return os.toByteArray();
	}
}

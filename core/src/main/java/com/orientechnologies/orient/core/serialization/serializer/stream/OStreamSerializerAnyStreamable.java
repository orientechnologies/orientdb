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

import java.io.IOException;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OArrays;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.OSerializableStream;

public class OStreamSerializerAnyStreamable implements OStreamSerializer {
	public static final OStreamSerializerAnyStreamable	INSTANCE	= new OStreamSerializerAnyStreamable();
	public static final String													NAME			= "at";

	/**
	 * Re-Create any object if the class has a public constructor that accepts a String as unique parameter.
	 */
	public Object fromStream(final ODatabaseRecord iDatabase, final byte[] iStream) throws IOException {
		if (iStream == null || iStream.length == 0)
			// NULL VALUE
			return null;

		final int classNameSize = OBinaryProtocol.bytes2int(iStream);
		if (classNameSize <= 0)
			OLogManager.instance().error(this, "Class signature not found in ANY element: " + iStream, OSerializationException.class);

		final String className = OBinaryProtocol.bytes2string(iStream, 4, classNameSize);

		try {
			Class<?> clazz = Class.forName(className);

			// CREATE THE OBJECT BY INVOKING THE EMPTY CONSTRUCTOR
			OSerializableStream stream = (OSerializableStream) clazz.newInstance();

			return stream.fromStream(OArrays.copyOfRange(iStream, 4 + classNameSize, iStream.length));

		} catch (Exception e) {
			OLogManager.instance().error(this, "Error on unmarshalling content. Class: " + className, e, OSerializationException.class);
		}
		return null;
	}

	/**
	 * Serialize the class name size + class name + object content
	 */
	public byte[] toStream(final ODatabaseRecord iDatabase, final Object iObject) throws IOException {
		if (iObject == null)
			return null;

		if (!(iObject instanceof OSerializableStream))
			throw new OSerializationException("Can't serialize the object [" + iObject.getClass() + ":" + iObject
					+ "] since it not implements the OSerializableStream interface");

		OSerializableStream stream = (OSerializableStream) iObject;

		// SERIALIZE THE CLASS NAME
		byte[] className = OBinaryProtocol.string2bytes(iObject.getClass().getName());
		
		// SERIALIZE THE OBJECT CONTENT
		byte[] objectContent = stream.toStream();

		byte[] result = new byte[4 + className.length + objectContent.length];

		// COPY THE CLASS NAME SIZE + CLASS NAME + OBJECT CONTENT
		System.arraycopy(OBinaryProtocol.int2bytes(className.length), 0, result, 0, 4);
		System.arraycopy(className, 0, result, 4, className.length);
		System.arraycopy(objectContent, 0, result, 4 + className.length, objectContent.length);

		return result;
	}

	public String getName() {
		return NAME;
	}
}

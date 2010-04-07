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
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;

public class OStreamSerializerAnyRuntime extends OStreamSerializerAbstract {
	public static final OStreamSerializerAnyRuntime	INSTANCE	= new OStreamSerializerAnyRuntime();
	private static final String											NAME			= "au";

	public String getName() {
		return NAME;
	}

	/**
	 * Re-Create any object if the class has a public constructor that accepts a String as unique parameter.
	 */
	public Object fromStream(byte[] iStream) throws IOException {
		if (iStream == null || iStream.length == 0)
			// NULL VALUE
			return null;

		String stream = OBinaryProtocol.bytes2string(iStream);
		int pos = stream.indexOf(SEPARATOR);
		if (pos < 0)
			OLogManager.instance().error(this, "Class signature not found in ANY element: " + iStream, OSerializationException.class);

		final String className = stream.substring(0, pos);

		try {
			Class<?> clazz = Class.forName(className);
			return clazz.getDeclaredConstructor(String.class).newInstance(stream.substring(pos + 1));
		} catch (Exception e) {
			OLogManager.instance().error(this, "Error on unmarshalling content. Class: " + className, e, OSerializationException.class);
		}
		return null;
	}

	public byte[] toStream(Object iObject) throws IOException {
		if (iObject == null)
			return null;
		return OBinaryProtocol.string2bytes(iObject.getClass().getName() + SEPARATOR + iObject);
	}
}

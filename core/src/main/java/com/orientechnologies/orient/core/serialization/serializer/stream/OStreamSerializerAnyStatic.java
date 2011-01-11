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
import java.lang.reflect.Constructor;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;

public class OStreamSerializerAnyStatic implements OStreamSerializer {
	public static final String	NAME	= "as";

	private Constructor<?>			constructor;

	public OStreamSerializerAnyStatic(final Class<?> iClazz) throws SecurityException, NoSuchMethodException {
		constructor = iClazz.getConstructor(String.class);
	}

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

		try {
			return constructor.newInstance(iStream);
		} catch (Exception e) {
			OLogManager.instance().error(this, "Error on unmarshalling content of class: " + constructor.getDeclaringClass(), e,
					OSerializationException.class);
		}
		return null;
	}

	public byte[] toStream(final ODatabaseRecord iDatabase, final Object iObject) throws IOException {
		if (iObject == null)
			return null;
		return OBinaryProtocol.string2bytes(iObject.toString());
	}
}

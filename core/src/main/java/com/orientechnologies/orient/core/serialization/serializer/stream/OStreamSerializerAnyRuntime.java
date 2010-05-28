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

import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringSerializerAnyRuntime;

/**
 * Delegates to the OStringSerializerAnyRuntime class but transform to/from bytes.
 * 
 * @see OStringSerializerAnyRuntime
 * @author Luca Garulli
 * 
 */
public class OStreamSerializerAnyRuntime implements OStreamSerializer {
	public static final OStreamSerializerAnyRuntime	INSTANCE	= new OStreamSerializerAnyRuntime();
	private static final String											NAME			= "au";

	public String getName() {
		return NAME;
	}

	/**
	 * Re-Create any object if the class has a public constructor that accepts a String as unique parameter.
	 */
	public Object fromStream(final byte[] iStream) throws IOException {
		if (iStream == null || iStream.length == 0)
			// NULL VALUE
			return null;

		return OStringSerializerAnyRuntime.INSTANCE.fromStream(OBinaryProtocol.bytes2string(iStream));
	}

	public byte[] toStream(final Object iObject) throws IOException {
		if (iObject == null)
			return new byte[0];

		return OBinaryProtocol.string2bytes(OStringSerializerAnyRuntime.INSTANCE.toStream(iObject));
	}
}

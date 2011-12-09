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

public class OStreamSerializerLong implements OStreamSerializer {
	public static final String								NAME			= "l";

	public static final OStreamSerializerLong	INSTANCE	= new OStreamSerializerLong();

	public String getName() {
		return NAME;
	}

	public Object fromStream(final byte[] iStream) throws IOException {
		return OBinaryProtocol.bytes2long(iStream);
	}

	public byte[] toStream(final Object iObject) throws IOException {
		return OBinaryProtocol.long2bytes((Long) iObject);
	}
}

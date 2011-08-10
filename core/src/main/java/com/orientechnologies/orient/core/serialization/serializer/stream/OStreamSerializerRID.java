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

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.id.ORecordId;

public class OStreamSerializerRID implements OStreamSerializer {
	public static final String								NAME			= "p";

	public static final OStreamSerializerRID	INSTANCE	= new OStreamSerializerRID();

	public String getName() {
		return NAME;
	}

	public Object fromStream(final ODatabaseRecord iDatabase, final byte[] iStream) throws IOException {
		if (iStream == null)
			return null;

		return new ORecordId().fromStream(iStream);
	}

	public byte[] toStream(final ODatabaseRecord iDatabase, final Object iObject) throws IOException {
		if (iObject == null)
			return null;

		return ((ORecordId) iObject).toStream();
	}
}

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

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;

public class OStreamSerializerRecord implements OStreamSerializer {
	public static final String									NAME			= "l";
	public static final OStreamSerializerRecord	INSTANCE	= new OStreamSerializerRecord();

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

		final ORecordInternal<?> obj = Orient.instance().getRecordFactoryManager().newInstance(iDatabase);

		final ORID rid = new ORecordId().fromStream(iStream);

		obj.setIdentity(rid.getClusterId(), rid.getClusterPosition());
		return obj;
	}

	public byte[] toStream(final ODatabaseRecord iDatabase, final Object iObject) throws IOException {
		if (iObject == null)
			return null;

		if (((ORecord<?>) iObject).getIdentity() == null)
			throw new OSerializationException("Cannot serialize record without identity. Store it before to serialize.");

		return ((ORecord<?>) iObject).getIdentity().toStream();
	}
}

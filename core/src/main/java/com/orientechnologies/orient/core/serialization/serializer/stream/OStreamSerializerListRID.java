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

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRIDSet;

public class OStreamSerializerListRID implements OStreamSerializer {
	public static final String														NAME			= "y";
	public static final OStreamSerializerListRID					INSTANCE	= new OStreamSerializerListRID();
	private static final ORecordSerializerSchemaAware2CSV	FORMAT		= (ORecordSerializerSchemaAware2CSV) ORecordSerializerFactory
																																			.instance().getFormat(ORecordSerializerSchemaAware2CSV.NAME);

	public Object fromStream(final byte[] iStream) throws IOException {
		if (iStream == null)
			return null;

		final String s = OBinaryProtocol.bytes2string(iStream);

		return (OMVRBTreeRIDSet) FORMAT.embeddedCollectionFromStream(null, OType.EMBEDDEDSET, null, OType.LINK, s);
	}

	public byte[] toStream(final Object iObject) throws IOException {
		if (iObject == null)
			return null;

		return ((OMVRBTreeRIDSet) iObject).toStream();
	}

	public String getName() {
		return NAME;
	}
}

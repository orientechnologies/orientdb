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
package com.orientechnologies.orient.core.query.nativ;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.query.OQueryAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OSerializableStream;

@SuppressWarnings("serial")
public abstract class ONativeQuery<CTX extends OQueryContextNative> extends OQueryAbstract<ODocument> {
	protected String	cluster;
	protected CTX		queryRecord;

	public abstract boolean filter(CTX iRecord);

	protected ONativeQuery(final ODatabaseRecord iDatabase, final String iCluster) {
		super(iDatabase);
		cluster = iCluster;
	}

	public byte[] toStream() throws OSerializationException {
		throw new OSerializationException("Native queries cannot be serialized");
	}

	public OSerializableStream fromStream(byte[] iStream) throws OSerializationException {
		throw new OSerializationException("Native queries cannot be deserialized");
	}

}

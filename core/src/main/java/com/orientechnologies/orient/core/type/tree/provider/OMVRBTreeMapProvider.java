/*
 * Copyright 1999-2011 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.type.tree.provider;

import java.io.IOException;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ORecordBytesLazy;
import com.orientechnologies.orient.core.serialization.OMemoryStream;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializer;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerFactory;
import com.orientechnologies.orient.core.storage.OStorage;

public class OMVRBTreeMapProvider<K, V> extends OMVRBTreeProviderBinary<K, V> {
	private static final long			serialVersionUID					= 1L;

	public final static byte			CURRENT_PROTOCOL_VERSION	= 0;

	protected final OMemoryStream	stream;
	protected OStreamSerializer		keySerializer;
	protected OStreamSerializer		valueSerializer;
	protected boolean							keepKeysInMemory;
	protected boolean							keepValuesInMemory;

	public OMVRBTreeMapProvider(final OStorage iStorage, final String iClusterName, final ORID iRID) {
		this(iStorage, iClusterName, null, null);
		record.setIdentity(iRID.getClusterId(), iRID.getClusterPosition());
	}

	public OMVRBTreeMapProvider(final OStorage iStorage, final String iClusterName, final OStreamSerializer iKeySerializer,
			final OStreamSerializer iValueSerializer) {
		super(new ORecordBytesLazy(), iStorage, iClusterName);
		((ORecordBytesLazy) record).recycle(this);
		stream = new OMemoryStream();
		keySerializer = iKeySerializer;
		valueSerializer = iValueSerializer;
	}

	public OMVRBTreeEntryDataProvider<K, V> getEntry(final ORID iRid) {
		return new OMVRBTreeMapEntryProvider<K, V>(this, iRid);
	}

	public OMVRBTreeEntryDataProvider<K, V> createEntry() {
		return new OMVRBTreeMapEntryProvider<K, V>(this);
	}

	@Override
	protected void load(final ODatabaseRecord iDb) {
		((ORecordBytesLazy) record).recycle(this);
		super.load(iDb);
	}

	@Override
	protected void load(final OStorage iSt) {
		((ORecordBytesLazy) record).recycle(this);
		super.load(iSt);
	}

	public boolean updateConfig() {
		final boolean changed = super.updateConfig();
		keepKeysInMemory = OGlobalConfiguration.MVRBTREE_ENTRY_KEYS_IN_MEMORY.getValueAsBoolean();
		keepValuesInMemory = OGlobalConfiguration.MVRBTREE_ENTRY_VALUES_IN_MEMORY.getValueAsBoolean();
		return changed;
	}

	public byte[] toStream() throws OSerializationException {
		final long timer = OProfiler.getInstance().startChrono();

		try {
			stream.jump(0);
			stream.set(CURRENT_PROTOCOL_VERSION);
			stream.setAsFixed(root != null ? root.toStream() : ORecordId.EMPTY_RECORD_ID_STREAM);

			stream.set(size);
			stream.set(defaultPageSize);

			stream.set(keySerializer.getName());
			stream.set(valueSerializer.getName());

			final byte[] result = stream.toByteArray();
			record.fromStream(result);
			return result;

		} finally {
			OProfiler.getInstance().stopChrono("OMVRBTreeMapProvider.toStream", timer);
		}
	}

	public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
		final long timer = OProfiler.getInstance().startChrono();
		try {
			stream.setSource(iStream);
			byte protocolVersion = stream.peek();
			if (protocolVersion != -1) {
				// @COMPATIBILITY BEFORE 0.9.25
				stream.getAsByte();
				if (protocolVersion != CURRENT_PROTOCOL_VERSION)
					throw new OSerializationException(
							"The index has been created with a previous version of OrientDB. Soft transitions between versions is supported since 0.9.25. To use it with this version of OrientDB you need to export and import your database. "
									+ protocolVersion + "<->" + CURRENT_PROTOCOL_VERSION);
			}

			root = new ORecordId();
			root.fromStream(stream.getAsByteArrayFixed(ORecordId.PERSISTENT_SIZE));

			size = stream.getAsInteger();
			if (protocolVersion == -1)
				// @COMPATIBILITY BEFORE 0.9.25
				defaultPageSize = stream.getAsShort();
			else
				defaultPageSize = stream.getAsInteger();

			serializerFromStream(stream);

		} catch (Exception e) {
			OLogManager.instance().error(this, "Error on unmarshalling OMVRBTreeMapProvider object from record: %s", e,
					OSerializationException.class, root);
		} finally {
			OProfiler.getInstance().stopChrono("OMVRBTreeMapProvider.fromStream", timer);
		}
		return this;
	}

	protected void serializerFromStream(final OMemoryStream stream) throws IOException {
		keySerializer = OStreamSerializerFactory.get(stream.getAsString());
		valueSerializer = OStreamSerializerFactory.get(stream.getAsString());
	}
}

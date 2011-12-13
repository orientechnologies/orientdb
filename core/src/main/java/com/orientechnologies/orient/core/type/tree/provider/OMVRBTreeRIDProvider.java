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

import java.util.StringTokenizer;

import com.orientechnologies.common.collection.OMVRBTree;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringBuilderSerializable;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.type.tree.OMVRBTreePersistent;
import com.orientechnologies.orient.core.type.tree.OMVRBTreeRID;

/**
 * MVRB-Tree implementation to handle a set of RID. It's serialized as embedded or external binary. Once external cannot come back
 * to the embedded mode.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OMVRBTreeRIDProvider extends OMVRBTreeProviderAbstract<OIdentifiable, OIdentifiable> implements
		OStringBuilderSerializable {
	private static final long		serialVersionUID	= 1L;

	private OMVRBTreeRID				tree;
	private boolean							embeddedStreaming	= true;								// KEEP THE STREAMING MODE
	private final StringBuilder	buffer						= new StringBuilder();
	private boolean							marshalling				= false;

	public OMVRBTreeRIDProvider(final OStorage iStorage, final int iClusterId, final ORID iRID) {
		this(iStorage, getDatabase().getClusterNameById(iClusterId));
		record.setIdentity(iRID.getClusterId(), iRID.getClusterPosition());
	}

	public OMVRBTreeRIDProvider(final OStorage iStorage, final String iClusterName, final ORID iRID) {
		this(iStorage, iClusterName);
		record.setIdentity(iRID.getClusterId(), iRID.getClusterPosition());
	}

	public OMVRBTreeRIDProvider(final OStorage iStorage, final int iClusterId) {
		this(iStorage, getDatabase().getClusterNameById(iClusterId));
	}

	public OMVRBTreeRIDProvider(final OStorage iStorage, final String iClusterName) {
		super(new ODocument(getDatabase()), iStorage, iClusterName);
		((ODocument) record).field("pageSize", pageSize);
	}

	public OMVRBTreeRIDEntryProvider getEntry(final ORID iRid) {
		return new OMVRBTreeRIDEntryProvider(this, iRid);
	}

	public OMVRBTreeRIDEntryProvider createEntry() {
		return new OMVRBTreeRIDEntryProvider(this);
	}

	public OStringBuilderSerializable toStream(final StringBuilder iBuffer) throws OSerializationException {
		final long timer = OProfiler.getInstance().startChrono();

		if (buffer.length() == 0)
			// MARSHALL IT
			try {
				marshalling = true;
				if (isEmbeddedStreaming()) {
					// SERIALIZE AS AN EMBEDDED STRING
					buffer.append(OStringSerializerHelper.COLLECTION_BEGIN);
					boolean first = true;
					for (OIdentifiable rid : tree.keySet()) {
						if (!first)
							buffer.append(OStringSerializerHelper.COLLECTION_SEPARATOR);
						else
							first = false;

						rid.getIdentity().toString(buffer);
					}
					buffer.append(OStringSerializerHelper.COLLECTION_END);
				} else {
					buffer.append(OStringSerializerHelper.EMBEDDED_BEGIN);
					buffer.append(new String(toDocument().toStream()));
					buffer.append(OStringSerializerHelper.EMBEDDED_END);
				}

			} finally {
				marshalling = false;
				OProfiler.getInstance().stopChrono("OMVRBTreeRIDProvider.toStream", timer);
			}

		iBuffer.append(buffer);

		return this;
	}

	public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
		record.fromStream(iStream);
		fromDocument((ODocument) record);
		return this;
	}

	public OStringBuilderSerializable fromStream(final StringBuilder iInput) throws OSerializationException {
		if (iInput != null) {
			// COPY THE BUFFER: IF THE TREE IS UNTOUCHED RETURN IT
			buffer.setLength(0);
			buffer.append(iInput);
		}

		return this;
	}

	public void lazyUnmarshall() {
		if (size > 0 || marshalling || buffer.length() == 0)
			// ALREADY UNMARSHALLED
			return;

		marshalling = true;

		try {
			final char firstChar = buffer.charAt(0);

			String value = firstChar == OStringSerializerHelper.COLLECTION_BEGIN ? buffer.substring(1, buffer.length() - 1) : buffer
					.toString();

			if (firstChar == OStringSerializerHelper.COLLECTION_BEGIN || firstChar == OStringSerializerHelper.LINK) {
				embeddedStreaming = true;
				final StringTokenizer tokenizer = new StringTokenizer(value, ",");
				while (tokenizer.hasMoreElements()) {
					final ORecordId rid = new ORecordId(tokenizer.nextToken());
					tree.put(rid, rid);
				}
			} else {
				embeddedStreaming = false;
				value = firstChar == OStringSerializerHelper.EMBEDDED_BEGIN ? value.substring(1, value.length() - 1) : value.toString();
				fromStream(value.getBytes());
				tree.load();
			}
		} finally {
			marshalling = false;
		}
	}

	public byte[] toStream() throws OSerializationException {
		return toDocument().toStream();
	}

	public OMVRBTree<OIdentifiable, OIdentifiable> getTree() {
		return tree;
	}

	public void setTree(final OMVRBTreePersistent<OIdentifiable, OIdentifiable> tree) {
		this.tree = (OMVRBTreeRID) tree;
	}

	@Override
	public boolean setDirty() {
		if (buffer != null)
			buffer.setLength(0);
		return super.setDirty();
	}

	public ODocument toDocument() {
		// SERIALIZE AS LINK TO THE TREE STRUCTURE
		((ODocument) record).setClassName("ORIDs");
		return ((ODocument) record).field("root", root != null ? root : null);
	}

	public void fromDocument(final ODocument iDocument) {
		pageSize = (Integer) iDocument.field("pageSize");
		root = iDocument.field("root", OType.LINK);
	}

	public boolean isEmbeddedStreaming() {
		if (embeddedStreaming) {
			final int binaryThreshold = OGlobalConfiguration.MVRBTREE_RID_BINARY_THRESHOLD.getValueAsInteger();
			if (binaryThreshold > 0 && size > binaryThreshold) {
				// CHANGE TO EXTERNAL BINARY
				tree.setDirtyOwner();
				embeddedStreaming = false;
			}
		}
		return embeddedStreaming;
	}

	@Override
	public boolean updateConfig() {
		boolean isChanged = false;

		int newSize = OGlobalConfiguration.MVRBTREE_RID_NODE_PAGE_SIZE.getValueAsInteger();
		if (newSize != pageSize) {
			pageSize = newSize;
			isChanged = true;
		}
		return isChanged ? setDirty() : false;
	}
}

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

import com.orientechnologies.common.collection.OMVRBTree;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OSerializationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringBuilderSerializable;
import com.orientechnologies.orient.core.storage.OStorage;

/**
 * MVRB-Tree implementation to handle a set of RID.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com) *
 */
public class OMVRBTreeRIDProvider extends OMVRBTreeProviderBinary<ORecordId, ORecordId> implements OStringBuilderSerializable {
	private static final long								serialVersionUID	= 1L;
	private static final int								PROTOCOL_VERSION	= 0;

	private OMVRBTree<ORecordId, ORecordId>	tree;
	private boolean													embeddedStreaming	= true;

	public OMVRBTreeRIDProvider(final OStorage iStorage, final String iClusterName, final ORID iRID) {
		this(iStorage, iClusterName);
		record.setIdentity(iRID.getClusterId(), iRID.getClusterPosition());
	}

	public OMVRBTreeRIDProvider(final OStorage iStorage, final String iClusterName) {
		super(new ODocument(), iStorage, iClusterName);
	}

	public OMVRBTreeRIDEntryProvider getEntry(final ORID iRid) {
		return new OMVRBTreeRIDEntryProvider(this, iRid);
	}

	public OMVRBTreeRIDEntryProvider createEntry() {
		return new OMVRBTreeRIDEntryProvider(this);
	}

	public void toStream(final StringBuilder buffer) throws OSerializationException {
		final long timer = OProfiler.getInstance().startChrono();

		try {
			if (embeddedStreaming && size > OGlobalConfiguration.MVRBTREE_SET_BINARY_THRESHOLD.getValueAsInteger())
				embeddedStreaming = false;

			if (embeddedStreaming) {
				// SERIALIZE AS AN EMBEDDED STRING
				buffer.append('[');
				for (ORecordId rid : tree.keySet()) {
					if (buffer.length() > 1)
						buffer.append(',');
					rid.toString(buffer);
				}
				buffer.append(']');
			} else {
				// SERIALIZE AS LINK TO THE TREE STRUCTURE
				final ODocument document = (ODocument) record;
				document.clear();

				document.field("protocolVersion", PROTOCOL_VERSION);
				document.field("size", size);
				document.field("defaultPageSize", defaultPageSize);
				if (root != null)
					document.field("root", root.getIdentity());

				buffer.append(document.toString());
			}

		} finally {
			OProfiler.getInstance().stopChrono("OMVRBTreeRIDProvider.toStream", timer);
		}
	}

	public void fromStream(final StringBuilder iInput) throws OSerializationException {
	}

	public OSerializableStream fromStream(final byte[] iStream) throws OSerializationException {
		throw new UnsupportedOperationException("toStream");
	}

	public byte[] toStream() throws OSerializationException {
		throw new UnsupportedOperationException("toStream");
	}

	public OMVRBTree<ORecordId, ORecordId> getTree() {
		return tree;
	}

	public void setTree(OMVRBTree<ORecordId, ORecordId> tree) {
		this.tree = tree;
	}
}

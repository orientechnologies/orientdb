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
package com.orientechnologies.orient.server.replication.conflict;

import java.util.Date;
import java.util.Map;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;
import com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo.SYNCH_TYPE;
import com.orientechnologies.orient.server.replication.OReplicator;

/**
 * Default conflict resolver.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class ODefaultDistributedConflictResolver implements ODistributedConflictResolver {

	public static final String	DISTRIBUTED_CONFLICT_CLASS	= "ODistributedConflict";
	private OReplicator					replicator;

	private boolean							ignoreIfSameContent;
	private boolean							ignoreIfMergeOk;
	private boolean							latestAlwaysWin;

	public void config(final OReplicator iReplicator, final Map<String, String> iConfig) {
		replicator = iReplicator;
		replicator.addIgnoredDocumentClass(DISTRIBUTED_CONFLICT_CLASS);
		replicator.addIgnoredCluster(OStorageLocal.CLUSTER_INTERNAL_NAME);
		replicator.addIgnoredCluster(OStorageLocal.CLUSTER_INDEX_NAME);

		ignoreIfSameContent = Boolean.parseBoolean(iConfig.get("ignoreIfSameContent"));
		ignoreIfMergeOk = Boolean.parseBoolean(iConfig.get("ignoreIfMergeOk"));
		latestAlwaysWin = Boolean.parseBoolean(iConfig.get("latestAlwaysWin"));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.server.replication.ODistributedConflict#handleCreateConflict(byte,
	 * com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo.SYNCH_TYPE,
	 * com.orientechnologies.orient.core.record.ORecordInternal, long)
	 */
	@Override
	public void handleCreateConflict(final byte iOperation, SYNCH_TYPE iRequestType, final ORecordInternal<?> iRecord,
			final long iOtherClusterPosition) {
		OLogManager.instance().warn(this, "-> %s (%s mode) CONFLICT record %s (other RID=#%d:%d)...", this, iRequestType,
				iRecord.getIdentity(), iRecord.getIdentity().getClusterId(), iOtherClusterPosition);

		final ODocument doc = createConflictDocument(iOperation, iRecord);
		doc.field("otherClusterPos", iOtherClusterPosition);
		doc.save();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.server.replication.ODistributedConflict#handleUpdateConflict(byte,
	 * com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo.SYNCH_TYPE,
	 * com.orientechnologies.orient.core.record.ORecordInternal, int, int)
	 */
	@Override
	public void handleUpdateConflict(final byte iOperation, SYNCH_TYPE iRequestType, final ORecordInternal<?> iRecord,
			final int iCurrentVersion, final int iOtherVersion) {
		OLogManager.instance().warn(this, "-> %s (%s mode) CONFLICT record %s (current=v%d, other=v%d)...", this, iRequestType,
				iRecord.getIdentity(), iOtherVersion, iCurrentVersion);

		final ODocument doc = createConflictDocument(iOperation, iRecord);
		doc.field("currentVersion", iCurrentVersion);
		doc.field("otherVersion", iOtherVersion);
		doc.save();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.orientechnologies.orient.server.replication.ODistributedConflict#handleDeleteConflict(byte,
	 * com.orientechnologies.orient.server.replication.ODistributedDatabaseInfo.SYNCH_TYPE,
	 * com.orientechnologies.orient.core.record.ORecordInternal)
	 */
	@Override
	public void handleDeleteConflict(final byte iOperation, SYNCH_TYPE iRequestType, final ORecordInternal<?> iRecord) {
		OLogManager.instance().warn(this, "-> %s (%s mode) CONFLICT record %s (cannot be deleted on other node)", this, iRequestType,
				iRecord.getIdentity());

		final ODocument doc = createConflictDocument(iOperation, iRecord);
		doc.save();
	}

	protected ODocument createConflictDocument(final byte iOperation, final ORecordInternal<?> iRecord) {
		OClass cls = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getSchema().getClass(DISTRIBUTED_CONFLICT_CLASS);
		if (cls == null) {
			cls = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getSchema().createClass(DISTRIBUTED_CONFLICT_CLASS);
			cls.createProperty("record", OType.LINK).createIndex(INDEX_TYPE.UNIQUE);
		}

		final ODocument doc = new ODocument(DISTRIBUTED_CONFLICT_CLASS);
		doc.field("operation", iOperation);
		doc.field("date", new Date());
		doc.field("record", iRecord.getIdentity());
		return doc;
	}

	@Override
	public String toString() {
		return replicator.getManager().getId();
	}
}

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
package com.orientechnologies.orient.kv.network.protocol.http.partitioned;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Instance;
import com.hazelcast.core.InstanceEvent;
import com.hazelcast.core.InstanceListener;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.Instance.InstanceType;
import com.hazelcast.partition.MigrationEvent;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.PartitionService;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.record.ODatabaseBinary;
import com.orientechnologies.orient.core.index.OTreeMapPersistent;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.serialization.serializer.stream.OStreamSerializerString;
import com.orientechnologies.orient.kv.OSharedDatabase;
import com.orientechnologies.orient.kv.network.protocol.http.ONetworkProtocolHttpKV;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerStorageConfiguration;

@SuppressWarnings("unchecked")
public class OServerClusterMember implements InstanceListener, MembershipListener, MigrationListener {

	private static final String	DEFAULT_CLUSTER_NAME	= "default";

	public OServerClusterMember() {
		OLogManager.instance().config(this, "Orient Server starting cluster...");
		Hazelcast.addInstanceListener(this);
		Hazelcast.getPartitionService().addMigrationListener(this);

		for (OServerStorageConfiguration stg : OServerMain.server().getConfiguration().storages) {
			OLogManager.instance().config(this, "- Registering distributed database: " + stg.name);
			Hazelcast.getMap(stg.name);
		}

		OLogManager.instance().config(this, "Orient Server cluster started successfully");
	}

	public void instanceCreated(InstanceEvent iEvent) {
		OLogManager.instance().debug(this, "Orient Server instance registered: %d", iEvent);
	}

	public void instanceDestroyed(InstanceEvent iEvent) {
		OLogManager.instance().debug(this, "Orient Server instance unregistered: %s", iEvent);
	}

	public void memberAdded(MembershipEvent iEvent) {
		OLogManager.instance().config(this, "Orient Server member added: %s", iEvent);
	}

	public void memberRemoved(MembershipEvent iEvent) {
		OLogManager.instance().config(this, "Orient Server member removed: %s", iEvent);
	}

	public void migrationStarted(MigrationEvent iEvent) {
		OLogManager.instance().debug(this, "Orient Server migration started for partition %d from %s to %s", iEvent.getPartitionId(),
				iEvent.getOldOwner(), iEvent.getNewOwner());
	}

	public void migrationCompleted(MigrationEvent iEvent) {
		OLogManager.instance().debug(this, "Orient Server migration completed for partition %d from %s to %s", iEvent.getPartitionId(),
				iEvent.getOldOwner(), iEvent.getNewOwner());

		final int partitionId = iEvent.getPartitionId();

		if (!iEvent.getNewOwner().equals(Hazelcast.getCluster().getLocalMember()))
			// PARTITION MIGRATED NOT TO MYSELF: PERSIST ALL THE ENTRIES
			return;

		String parts[];
		Map<String, String> bucket;

		for (Instance instance : Hazelcast.getInstances()) {
			if (instance.getInstanceType() == InstanceType.MAP) {
				IMap<String, String> map = (IMap<String, String>) instance;

				PartitionService partitionService = Hazelcast.getPartitionService();
				Set<String> localKeys = map.localKeySet();
				for (String localKey : localKeys) {

					if (partitionService.getPartition(localKey).getPartitionId() == partitionId) {
						// MY OWN ENTRY: STORE IT
						try {
							parts = ONetworkProtocolHttpKV.getDbBucketKey(localKey, 3);
							ODatabaseBinary db = OSharedDatabase.acquireDatabase(parts[0]);

							bucket = getDictionaryBucket(db, parts[1]);

							bucket.put(localKey, map.get(localKey));

							OLogManager.instance().debug(this, "Caught failure of node '%s'. Save entry with key '%s' on local node",
									iEvent.getOldOwner().getInetSocketAddress(), localKey);

						} catch (Exception e) {
							throw new ODistributedException("Error on saving record: " + localKey, e);
						}
					}
				}
			}
		}
	}

	public static Map<String, String> getDictionaryBucket(final ODatabaseBinary iDatabase, final String iName) throws IOException {
		ORecordBytes record = iDatabase.getDictionary().get(iName);

		OTreeMapPersistent<String, String> bucket;

		if (record == null) {
			// CREATE THE BUCKET TRANSPARENTLY
			bucket = new OTreeMapPersistent<String, String>(iDatabase, DEFAULT_CLUSTER_NAME, OStreamSerializerString.INSTANCE,
					OStreamSerializerString.INSTANCE);
			bucket.save();
			// REGISTER THE NEW BUCKET
			iDatabase.getDictionary().put(iName, bucket.getRecord());
		} else {
			bucket = new OTreeMapPersistent<String, String>(iDatabase, DEFAULT_CLUSTER_NAME, record.getIdentity());
			bucket.load();
		}

		return bucket;
	}
}
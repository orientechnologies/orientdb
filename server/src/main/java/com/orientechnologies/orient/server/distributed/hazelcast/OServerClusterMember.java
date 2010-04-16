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
package com.orientechnologies.orient.server.distributed.hazelcast;

import java.util.Set;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.core.InstanceEvent;
import com.hazelcast.core.InstanceListener;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.partition.MigrationEvent;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.PartitionService;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.OServerStorageConfiguration;

public class OServerClusterMember implements InstanceListener, MembershipListener, MigrationListener {

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
		OLogManager.instance().debug(this, "Orient Server member added: %s", iEvent);
	}

	public void memberRemoved(MembershipEvent iEvent) {
		OLogManager.instance().debug(this, "Orient Server member removed: %s", iEvent);
	}

	public void migrationStarted(MigrationEvent iEvent) {
		OLogManager.instance().config(this, "Orient Server migration started for partition %d from %s to %s", iEvent.getPartitionId(),
				iEvent.getOldOwner(), iEvent.getNewOwner());
	}

	public void migrationCompleted(MigrationEvent iEvent) {
		OLogManager.instance().config(this, "Orient Server migration completed for partition %d from %s to %s",
				iEvent.getPartitionId(), iEvent.getOldOwner(), iEvent.getNewOwner());

		final int partitionId = iEvent.getPartitionId();

		if (iEvent.getNewOwner().equals(Hazelcast.getCluster().getLocalMember())) {
			// PARTITION MIGRATED TO MYSELF: PERSIST ALL THE ENTRIES
			OLogManager.instance().config(this, "The new member is the current node, assure to store all the keys", iEvent);
		}

//		IMap<?, ?> map = (IMap<?, ?>) iEvent.getSource();
//
//		PartitionService partitionService = Hazelcast.getPartitionService();
//		Set<?> localKeys = map.localKeySet();
//		for (Object localKey : localKeys) {
//			if (partitionService.getPartition(localKey).getPartitionId() == partitionId) {
//			}
//		}
	}
}
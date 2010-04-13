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
package com.orientechnologies.orient.kv.hazelcast;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.InstanceEvent;
import com.hazelcast.core.InstanceListener;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.partition.MigrationEvent;
import com.hazelcast.partition.MigrationListener;
import com.orientechnologies.common.log.OLogManager;

public class OServerClusterMember implements InstanceListener, MembershipListener, MigrationListener {

	public OServerClusterMember() {
		OLogManager.instance().config(this, "Orient KV-Server starting cluster...");
		Hazelcast.addInstanceListener(this);
		Hazelcast.getPartitionService().addMigrationListener(this);
		OLogManager.instance().config(this, "Orient KV-Server cluster started successfully");
	}

	public void instanceCreated(InstanceEvent iEvent) {
		OLogManager.instance().config(this, "Orient KV-Server instance registered: " + iEvent);
		MapConfig cfg = Hazelcast.getConfig().getMapConfig(iEvent.getSource().toString());
	}

	public void instanceDestroyed(InstanceEvent iEvent) {
		OLogManager.instance().config(this, "Orient KV-Server instance unregistered: " + iEvent);
	}

	public void memberAdded(MembershipEvent iEvent) {
		OLogManager.instance().config(this, "Orient KV-Server member added: " + iEvent);

	}

	public void memberRemoved(MembershipEvent iEvent) {
		OLogManager.instance().config(this, "Orient KV-Server member removed: " + iEvent);
	}

	public void migrationStarted(MigrationEvent iEvent) {
		OLogManager.instance().debug(this, "Orient KV-Server migration started: " + iEvent);
	}

	public void migrationCompleted(MigrationEvent iEvent) {
		OLogManager.instance().debug(this, "Orient KV-Server migration completed: " + iEvent);
	}
}
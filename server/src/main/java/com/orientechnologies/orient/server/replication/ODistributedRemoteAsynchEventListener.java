package com.orientechnologies.orient.server.replication;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.enterprise.channel.binary.ORemoteServerEventListener;
import com.orientechnologies.orient.server.handler.distributed.OClusterProtocol;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerManager;

public class ODistributedRemoteAsynchEventListener implements ORemoteServerEventListener {

	private ODistributedServerManager		manager;
	private ORemoteServerEventListener	wrapped;
	private String											nodeId;

	public ODistributedRemoteAsynchEventListener(final ODistributedServerManager iServerManager,
			final ORemoteServerEventListener iToWrap, final String iNodeId) {
		this.manager = iServerManager;
		this.wrapped = iToWrap;
		this.nodeId = iNodeId;
	}

	public void onRequest(final byte iRequestCode, final Object obj) {
		if (iRequestCode == OClusterProtocol.PUSH_LEADER_AVAILABLE_DBS) {

			try {
				ODocument updateCfg = manager.getLeader().updatePeerDatabases(nodeId, (ODocument) obj);
				manager.getReplicator().updateConfiguration(updateCfg);

			} catch (Exception e) {
				OLogManager.instance().error(this, "CLUSTER <%s>: error on updating leader's database list for peer %s", e,
						manager.getConfig().name, nodeId);
			}

		} else if (wrapped != null)
			// CALL THE WRAPPED INSTANCE
			wrapped.onRequest(iRequestCode, obj);
	}
}

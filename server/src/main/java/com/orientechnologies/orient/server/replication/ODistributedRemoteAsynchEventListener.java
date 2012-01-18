package com.orientechnologies.orient.server.replication;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.enterprise.channel.binary.ORemoteServerEventListener;
import com.orientechnologies.orient.enterprise.channel.distributed.OChannelDistributedProtocol;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerManager;

public class ODistributedRemoteAsynchEventListener implements ORemoteServerEventListener {

	private ODistributedServerManager		manager;
	private ORemoteServerEventListener	wrapped;
	private String											clientId;

	public ODistributedRemoteAsynchEventListener(final ODistributedServerManager iServerManager,
			final ORemoteServerEventListener iToWrap, final String iClientId) {
		this.manager = iServerManager;
		this.wrapped = iToWrap;
		this.clientId = iClientId;
	}

	public void onRequest(final byte iRequestCode, final Object obj) {
		if (iRequestCode == OChannelDistributedProtocol.PUSH_LEADER_AVAILABLE_DBS) {

			try {
				ODocument updateCfg = manager.getLeader().updatePeerDatabases(clientId, (ODocument) obj);
				manager.getReplicator().updateConfiguration(updateCfg);

			} catch (Exception e) {
				OLogManager.instance().error(this, "Cluster <%s>: error on updating leader's database list for peer %s", e,
						manager.getConfig().name, clientId);
			}

		} else if (wrapped != null)
			// CALL THE WRAPPED INSTANCE
			wrapped.onRequest(iRequestCode, obj);
	}
}

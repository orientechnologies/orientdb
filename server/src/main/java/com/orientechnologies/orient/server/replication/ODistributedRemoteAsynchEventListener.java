package com.orientechnologies.orient.server.replication;

import java.net.UnknownHostException;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.enterprise.channel.binary.ORemoteServerEventListener;
import com.orientechnologies.orient.enterprise.channel.distributed.OChannelDistributedProtocol;
import com.orientechnologies.orient.server.handler.distributed.ODistributedServerManager;

public class ODistributedRemoteAsynchEventListener implements ORemoteServerEventListener {

	private ODistributedServerManager		manager;
	private ORemoteServerEventListener	storage;

	public ODistributedRemoteAsynchEventListener(final ODistributedServerManager iServerManager,
			final ORemoteServerEventListener iToWrap) {
		this.manager = iServerManager;
		this.storage = iToWrap;
	}

	public void onRequest(final byte iRequestCode, final Object obj) {
		if (iRequestCode == OChannelDistributedProtocol.PUSH_LEADER_AVAILABLE_DBS) {
			ODocument doc = (ODocument) obj;
			try {
				manager.getLeader().addServerInConfiguration((String) doc.field("database"), (String) doc.field("node"),
						(String) doc.field("mode"));
			} catch (UnknownHostException e) {
				OLogManager.instance().error(this, "Cluster <%s>: error or updating configuration for node %s", e, doc.field("node"));
			}
		} else
			// CALL THE WRAPPED INSTANCE
			storage.onRequest(iRequestCode, obj);
	}
}

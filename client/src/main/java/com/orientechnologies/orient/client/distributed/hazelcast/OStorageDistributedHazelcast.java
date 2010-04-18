package com.orientechnologies.orient.client.distributed.hazelcast;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Instance;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.client.distributed.OStorageDistributed;
import com.orientechnologies.orient.enterprise.distributed.ODistributedException;

public class OStorageDistributedHazelcast extends OStorageDistributed {

	private static final String		CLUSTER_NAME			= "orient";
	private static final String		CLUSTER_PASSWORD	= "orient";
	private static final String[]	DEFAULT_PORTS			= new String[] { "5701", "8000" };

	private HazelcastInstance			client;

	public OStorageDistributedHazelcast(String iName, String iFilePath, String iMode) throws IOException {
		super(iName, iMode);
	}

	public void open(final int iRequesterId, final String iUserName, final String iUserPassword) {
		parseServerURLs();
		connectToCluster();

		for (Instance i : client.getInstances()) {
			if (i.getId().equals("c:" + name)) {
				super.open(iRequesterId, iUserName, iUserPassword);
				return;
			}
		}

		throw new ODistributedException("No partitioned instances of requested database are actives.");
	}

	@Override
	public void close() {
		super.close();

		client.shutdown();
	}

	public List<OPair<String, String[]>> getRemoteServers() {
		return Collections.unmodifiableList(serverURLs);
	}

	@Override
	protected String[] getDefaultPort() {
		return DEFAULT_PORTS;
	}

	protected Map<Object, Object> getDistributedDatabaseMap() {
		checkOpeness();
		return client.getMap(name);
	}

	protected void checkOpeness() {
		if (client == null)
			throw new ODistributedException("The client is not attached to a distributed cluster");
	}

	private void connectToCluster() {
		String[] addresses = new String[serverURLs.size()];
		int i = 0;
		for (OPair<String, String[]> server : serverURLs) {
			addresses[i++] = server.getKey() + ":" + server.getValue()[0];
		}

		OLogManager.instance().debug(this, "Trying to connect to the cluster node %s...", addresses[0]);
		client = HazelcastClient.newHazelcastClient(CLUSTER_NAME, CLUSTER_PASSWORD, addresses[0]);
	}
}

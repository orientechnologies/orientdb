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
package com.orientechnologies.orient.client.remote;

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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.concur.resource.OResourcePool;
import com.orientechnologies.common.concur.resource.OResourcePoolListener;
import com.orientechnologies.orient.enterprise.channel.OChannel;

public class ONetworkConnectionPool<CH extends OChannel> implements OResourcePoolListener<String, CH> {

	private static final int															DEF_WAIT_TIMEOUT	= 5000;
	private final Map<String, OResourcePool<String, CH>>	pools							= new HashMap<String, OResourcePool<String, CH>>();
	private int																						maxSize;
	private int																						timeout						= DEF_WAIT_TIMEOUT;

	public ONetworkConnectionPool(final int iMinSize, final int iMaxSize) {
		this(iMinSize, iMaxSize, DEF_WAIT_TIMEOUT);
	}

	public ONetworkConnectionPool(final int iMinSize, final int iMaxSize, final int iTimeout) {
		maxSize = iMaxSize;
		timeout = iTimeout;
	}

	public CH createNewResource(String iKey, Object... iAdditionalArgs) {
		return null;
	}

	public CH acquire(final String iRemoteAddress) throws OLockException {
		OResourcePool<String, CH> pool = pools.get(iRemoteAddress);
		if (pool == null) {
			synchronized (pools) {
				pool = pools.get(iRemoteAddress);
				if (pool == null) {
					pool = new OResourcePool<String, CH>(maxSize, this);
					pools.put(iRemoteAddress, pool);
				}
			}
		}

		return pool.getResource(iRemoteAddress, timeout);
	}

	public void release(final CH iChannel) {
		final String address = iChannel.socket.getInetAddress().toString();

		final OResourcePool<String, CH> pool = pools.get(address);
		if (pool == null)
			throw new OLockException("Can't release a network channel not acquired before. Remote address: " + address);

		pool.returnResource(iChannel);
	}

	public CH reuseResource(final String iKey, final Object[] iAdditionalArgs, final CH iValue) {
		return iValue;
	}

	public Map<String, OResourcePool<String, CH>> getPools() {
		return pools;
	}

	/**
	 * Closes all the channels.
	 */
	public void close() {
		for (Entry<String, OResourcePool<String, CH>> pool : pools.entrySet()) {
			for (CH channel : pool.getValue().getResources()) {
				channel.close();
			}
		}
	}
}

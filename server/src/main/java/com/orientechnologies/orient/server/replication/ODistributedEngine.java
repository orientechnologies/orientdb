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
package com.orientechnologies.orient.server.replication;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.remote.OStorageRemoteThread;
import com.orientechnologies.orient.core.engine.OEngineAbstract;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.storage.OStorage;

public class ODistributedEngine extends OEngineAbstract {
	public static final String														NAME						= "remote";
	private static final Map<String, ODistributedStorage>	sharedStorages	= new ConcurrentHashMap<String, ODistributedStorage>();

	private final OReplicator															replicator;

	public ODistributedEngine(final OReplicator iReplicator) {
		replicator = iReplicator;
	}

	public OStorage createStorage(final String iURL, final Map<String, String> iConfiguration) {
		try {
			synchronized (sharedStorages) {
				ODistributedStorage sharedStorage = sharedStorages.get(iURL);
				if (sharedStorage == null) {
					sharedStorage = new ODistributedStorage(iURL, "rw", replicator.getConflictResolver());
					sharedStorages.put(iURL, sharedStorage);
				}

				return new OStorageRemoteThread(sharedStorage);
			}
		} catch (Throwable t) {
			OLogManager.instance().error(this, "Error on opening database: " + iURL, t, ODatabaseException.class);
		}
		return null;
	}

	public void removeStorage(final String iURL) {
		synchronized (sharedStorages) {
			sharedStorages.remove(iURL);
		}
	}

	@Override
	public void removeStorage(final OStorage iStorage) {
		synchronized (sharedStorages) {
			for (Entry<String, ODistributedStorage> entry : sharedStorages.entrySet()) {
				if (entry.getValue() == iStorage) {
					sharedStorages.remove(entry.getKey());
					break;
				}
			}
		}
	}

	public String getName() {
		return NAME;
	}

	public boolean isShared() {
		return false;
	}
}

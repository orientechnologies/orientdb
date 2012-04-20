/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.storage;

import java.util.Arrays;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageMemoryClusterConfiguration;
import com.orientechnologies.orient.core.config.OStoragePhysicalClusterConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.impl.local.OClusterLocal;
import com.orientechnologies.orient.core.storage.impl.memory.OClusterMemory;

public class ODefaultClusterFactory implements OClusterFactory {
	private static final String[]	TYPES	= { "PHYSICAL", "MEMORY" };

	public OCluster createCluster(final String iType) {
		if (iType.equalsIgnoreCase("PHYSICAL"))
			return new OClusterLocal();
		else if (iType.equalsIgnoreCase("MEMORY"))
			return new OClusterMemory();
		else
			OLogManager.instance().exception(
					"Cluster type '" + iType + "' is not supported. Supported types are: " + Arrays.toString(TYPES), null,
					OStorageException.class);
		return null;
	}

	public OCluster createCluster(final OStorageClusterConfiguration iConfig) {
		if (iConfig instanceof OStoragePhysicalClusterConfiguration)
			return new OClusterLocal();
		else if (iConfig instanceof OStorageMemoryClusterConfiguration)
			return new OClusterMemory();
		else
			OLogManager.instance().exception(
					"Cluster type '" + iConfig + "' is not supported. Supported types are: " + Arrays.toString(TYPES), null,
					OStorageException.class);
		return null;
	}

	public String[] getSupported() {
		return TYPES;
	}

}

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
package com.orientechnologies.orient.client.distributed;

import java.util.Map;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.client.distributed.hazelcast.OStorageDistributedHazelcast;
import com.orientechnologies.orient.core.engine.OEngineAbstract;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.storage.OStorage;

public class OEngineDistributed extends OEngineAbstract {
	public static final String	NAME	= "distributed";

	public OEngineDistributed() {
	}

	public OStorage getStorage(final String iURL, final Map<String, String> iConfiguration) {
		try {
			return new OStorageDistributedHazelcast(iURL, iURL, "rw");
		} catch (Throwable t) {
			OLogManager.instance().error(this, "Error on opening database: " + iURL, t, ODatabaseException.class);
		}
		return null;
	}

	public String getName() {
		return NAME;
	}
}

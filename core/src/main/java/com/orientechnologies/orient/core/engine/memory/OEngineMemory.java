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
package com.orientechnologies.orient.core.engine.memory;

import java.util.Map;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.engine.OEngineAbstract;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.memory.OStorageMemory;

public class OEngineMemory extends OEngineAbstract {
	public static final String	NAME	= "memory";

	public OEngineMemory() {
	}

	public OStorage createStorage(String iURL, Map<String, String> iConfiguration) {
		try {
			return new OStorageMemory(iURL);
		} catch (Throwable t) {
			OLogManager.instance().error(this, "Error on opening in memory storage: " + iURL, t, ODatabaseException.class);
		}
		return null;
	}

	public String getName() {
		return NAME;
	}

	public boolean isShared() {
		return true;
	}
}

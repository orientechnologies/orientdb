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
package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.common.concur.resource.OSharedResourceAdaptive;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

public abstract class OSegment extends OSharedResourceAdaptive {
	protected OStorageLocal	storage;
	protected String				name;

	public OSegment(final OStorageLocal iStorage, String iName) {
		super(OGlobalConfiguration.ENVIRONMENT_CONCURRENT.getValueAsBoolean());
		storage = iStorage;
		name = iName;
	}

	public String getName() {
		return name;
	}
}

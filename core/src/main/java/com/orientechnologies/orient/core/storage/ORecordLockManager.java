/*
 * Copyright 1999-2011 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import com.orientechnologies.common.concur.lock.OLockManager;
import com.orientechnologies.orient.core.id.ORID;

/**
 * Record lock manager.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class ORecordLockManager extends OLockManager<ORID, Runnable> {

	public ORecordLockManager(final int iAcquireTimeout) {
		super(iAcquireTimeout);
	}

	@Override
	protected ORID getImmutableResourceId(ORID iResourceId) {
		return iResourceId.copy();
	}
}

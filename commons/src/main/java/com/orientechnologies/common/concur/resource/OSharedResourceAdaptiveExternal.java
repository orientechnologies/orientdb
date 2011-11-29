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
package com.orientechnologies.common.concur.resource;

/**
 * Optimize locks since they are enabled only when the engine runs in MULTI-THREADS mode.
 * 
 * @author Luca Garulli
 * 
 */
public class OSharedResourceAdaptiveExternal extends OSharedResourceAdaptive implements OSharedResource {
	public OSharedResourceAdaptiveExternal(final boolean iConcurrent, final int iTimeout) {
		super(iConcurrent, iTimeout);
	}

	@Override
	public void acquireExclusiveLock() {
		super.acquireExclusiveLock();
	}

	@Override
	public void acquireSharedLock() {
		super.acquireSharedLock();
	}

	@Override
	public void releaseExclusiveLock() {
		super.releaseExclusiveLock();
	}

	@Override
	public void releaseSharedLock() {
		super.releaseSharedLock();
	}
}

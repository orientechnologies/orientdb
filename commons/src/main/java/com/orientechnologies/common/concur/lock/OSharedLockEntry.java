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
package com.orientechnologies.common.concur.lock;

/**
 * Manages the shared lock requester list for the same resource.
 * 
 * @author Sylvain Spinelli (sylvain.spinelli@kelis.fr)
 * 
 * @param <REQUESTER_TYPE>
 */
public class OSharedLockEntry<REQUESTER_TYPE> {

	/** The requester lock : generally {@link Thread} or {@link Runnable}. */
	protected REQUESTER_TYPE										requester;

	/**
	 * Count shared locks held by this requester for the resource.
	 * <p>
	 * Used for reentrancy : when the same requester acquire a shared lock for the same resource in a nested code.
	 */
	protected int																countSharedLocks;

	/** Next shared lock for the same resource by an other requester. */
	protected OSharedLockEntry<REQUESTER_TYPE>	nextSharedLock;

	protected OSharedLockEntry() {
	}

	public OSharedLockEntry(final REQUESTER_TYPE iRequester) {
		super();
		requester = iRequester;
		countSharedLocks = 1;
	}
}
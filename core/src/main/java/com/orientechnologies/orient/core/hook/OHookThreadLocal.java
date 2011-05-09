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
package com.orientechnologies.orient.core.hook;

import java.util.HashSet;
import java.util.Set;

import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * Avoid recursion when hooks call themselves on the same record instances.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OHookThreadLocal extends ThreadLocal<Set<OIdentifiable>> {
	public static OHookThreadLocal	INSTANCE	= new OHookThreadLocal();

	@Override
	protected Set<OIdentifiable> initialValue() {
		return new HashSet<OIdentifiable>();
	}

	public boolean push(final OIdentifiable iRecord) {
		final Set<OIdentifiable> set = get();
		if (set.contains(iRecord))
			return false;

		set.add(iRecord);
		return true;
	}

	public boolean pop(final OIdentifiable iRecord) {
		return get().remove(iRecord);
	}
}
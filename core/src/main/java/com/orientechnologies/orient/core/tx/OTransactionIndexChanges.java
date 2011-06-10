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
package com.orientechnologies.orient.core.tx;

import java.util.HashMap;
import java.util.Map;

/**
 * Collects the changes to an index for a certain key
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OTransactionIndexChanges {

	public static enum OPERATION {
		PUT, REMOVE, CLEAR
	};

	public Map<Object, OTransactionIndexChangesPerKey>	changesPerKey	= new HashMap<Object, OTransactionIndexChangesPerKey>();
	public boolean																			cleared				= false;

	public OTransactionIndexChangesPerKey getChangesPerKey(final Object iKey) {
		OTransactionIndexChangesPerKey changes = changesPerKey.get(iKey);
		if (changes == null) {
			changes = new OTransactionIndexChangesPerKey(iKey);
			changesPerKey.put(iKey, changes);
		}

		return changes;
	}

	public void setCleared() {
		changesPerKey.clear();
		cleared = true;
	}

	public boolean containsChangesPerKey(final Object iKey) {
		return changesPerKey.containsKey(iKey);
	}
}

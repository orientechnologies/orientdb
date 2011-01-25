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
package com.orientechnologies.orient.core.index;

import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.id.ORecordId;

/**
 * Abstract not unique index class.
 * 
 * @author Luca Garulli
 * 
 */
public class OIndexNotUnique extends OIndexMVRBTreeAbstract {
	public OIndexNotUnique() {
		super("NOTUNIQUE");
	}

	public OIndex put(final Object iKey, final ORecordId iSingleValue) {
		List<ORecordId> values = map.get(iKey);
		if (values == null)
			values = new ArrayList<ORecordId>();

		int pos = values.indexOf(iSingleValue);
		if (pos > -1)
			// REPLACE IT
			values.set(pos, iSingleValue);
		else
			values.add(iSingleValue);

		map.put(iKey.toString(), values);
		return this;
	}
}

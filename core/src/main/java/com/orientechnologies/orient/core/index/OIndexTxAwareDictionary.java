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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;

/**
 * Transactional wrapper for dictionary index. Stores changes locally to the transaction until tx.commit(). All the other operations
 * are delegated to the wrapped OIndex instance.
 * 
 * @author Luca Garulli
 * 
 */
public class OIndexTxAwareDictionary extends OIndexTxAwareOneValue {
	public OIndexTxAwareDictionary(ODatabaseRecord iDatabase, OIndex<OIdentifiable> iDelegate) {
		super(iDatabase, iDelegate);
	}

	@Override
	public void checkEntry(final OIdentifiable iRecord, final Object iKey) {
	}
}

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
package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.record.impl.ORecordFlat;

/**
 * Delegates all the CRUD operations to the current transaction.
 * 
 */
public class ODatabaseFlat extends ODatabaseRecordTx {

	public ODatabaseFlat(String iURL) {
		super(iURL, ORecordFlat.RECORD_TYPE);
	}

	@SuppressWarnings("unchecked")
	@Override
	public ORecordIteratorCluster<ORecordFlat> browseCluster(final String iClusterName) {
		return super.browseCluster(iClusterName, ORecordFlat.class);
	}

	@SuppressWarnings("unchecked")
	@Override
	public ORecordFlat newInstance() {
		return new ORecordFlat(this);
	}

	@Override
	public ODatabaseRecord commit() {
		try {
			return super.commit();
		} finally {
			getTransaction().close();
		}
	}

	@Override
	public ODatabaseRecord rollback() {
		try {
			return super.rollback();
		} finally {
			getTransaction().close();
		}
	}

}

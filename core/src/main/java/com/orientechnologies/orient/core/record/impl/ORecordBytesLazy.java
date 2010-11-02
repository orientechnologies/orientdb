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
package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.tx.OTransactionEntry;
import com.orientechnologies.orient.core.tx.OTxListener;

/**
 * Extension of ORecordBytes that handle lazy serialization and converts temporary links (record id in transactions) to finals.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
@SuppressWarnings("unchecked")
public class ORecordBytesLazy extends ORecordBytes implements OTxListener {
	private OSerializableStream	rbpTreeNode;

	public ORecordBytesLazy(final OSerializableStream rbpTreeNode) {
		this.rbpTreeNode = rbpTreeNode;
	}

	@Override
	public byte[] toStream() {
		if (_source == null)
			_source = rbpTreeNode.toStream();
		return _source;
	}

	@Override
	public ORecordBytes copy() {
		final ORecordBytesLazy cloned = new ORecordBytesLazy(rbpTreeNode);
		cloned._source = null;
		cloned._database = _database;
		cloned._recordId = _recordId.copy();
		cloned._version = _version;
		return cloned;
	}

	public void onEvent(final OTransactionEntry<? extends ORecord<?>> iTxEntry, final EVENT iEvent) {
		switch (iEvent) {
		case BEFORE_COMMIT:
			// CONVERT TEMPORARY LINKS TO FINALS
			toStream();
		}
	}
}

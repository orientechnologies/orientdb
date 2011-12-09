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
package com.orientechnologies.orient.core.query.nativ;

import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

@SuppressWarnings("serial")
public abstract class ONativeSynchQuery<CTX extends OQueryContextNative> extends ONativeAsynchQuery<CTX> implements
		OCommandResultListener {
	protected final List<ODocument>	result	= new ArrayList<ODocument>();

	public ONativeSynchQuery(final ODatabaseRecord iDatabase, final String iCluster, final CTX iQueryRecordImpl) {
		super(iCluster, iQueryRecordImpl, null);
		resultListener = this;
	}

	public boolean result(final Object iRecord) {
		result.add((ODocument) iRecord);
		return true;
	}

	@Override
	public List<ODocument> run(final Object... iArgs) {
		super.run();
		return result;
	}

	@Override
	public ODocument runFirst(final Object... iArgs) {
		super.run();
		return result != null && !result.isEmpty() ? result.get(0) : null;
	}
}

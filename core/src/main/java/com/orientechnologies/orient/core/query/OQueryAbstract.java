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
package com.orientechnologies.orient.core.query;

import com.orientechnologies.orient.core.command.OCommandRequestAbstract;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.id.ORecordId;

@SuppressWarnings("serial")
public abstract class OQueryAbstract<T extends Object> extends OCommandRequestAbstract implements OQuery<T> {
	protected ORecordId	beginRange	= new ORecordId();
	protected ORecordId	endRange		= new ORecordId();
	protected String		fetchPlan;

	public OQueryAbstract() {
	}

	public OQueryAbstract(final ODatabaseRecord iDatabase) {
		super(iDatabase);
	}

	@SuppressWarnings("unchecked")
	public <RET> RET execute(final Object... iArgs) {
		return (RET) run(iArgs);
	}

	public String getFetchPlan() {
		return fetchPlan;
	}

	public OQuery<T> setFetchPlan(final String fetchPlan) {
		OFetchHelper.checkFetchPlanValid(fetchPlan);
		if (fetchPlan != null && fetchPlan.length() == 0)
			this.fetchPlan = null;
		else
			this.fetchPlan = fetchPlan;
		return this;
	}

	public ORecordId getBeginRange() {
		return beginRange;
	}

	public void setBeginRange(final ORecordId beginRange) {
		this.beginRange = beginRange;
	}

	public ORecordId getEndRange() {
		return endRange;
	}

	public void setEndRange(final ORecordId endRange) {
		this.endRange = endRange;
	}

	@Override
	public void reset() {
	}
}

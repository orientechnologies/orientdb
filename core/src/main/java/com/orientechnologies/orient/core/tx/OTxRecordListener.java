/*
 * Copyright 1999-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import com.orientechnologies.orient.core.record.ORecord;

/**
 * Listener that is called before and after record operations inside of transaction .
 * It is not called when record changes are committed only when operations on records inside transaction are performed.
 * It is used both for non- and optimistic transactions.
 */
public interface OTxRecordListener {
	public void onBeforeCreateRecordTx(final ORecord<?> iRecord);
	public void onAfterCreateRecordTx(final ORecord<?> iRecord);
	public void onBeforeUpdateRecordTx(final ORecord<?> iRecord);
	public void onAfterUpdateRecordTx(final ORecord<?> iRecord);
	public void onBeforeDeleteRecordTx(final ORecord<?> iRecord);
	public void onAfterDeleteRecordTx(final ORecord<?> iRecord);
	public void onBeforeLoadRecordTx(final ORecord<?> iRecord);
	public void onAfterLoadRecordTx(final ORecord<?> iRecord);
}

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
package com.orientechnologies.orient.core.hook;

import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Hook abstract class that calls separate methods for ODocument records.
 * 
 * @author Luca Garulli
 * @see ORecordHook
 */
public abstract class ODocumentHookAbstract implements ORecordHook {
	public void onRecordBeforeCreate(final ODocument iDocument) {
	};

	public void onRecordAfterCreate(final ODocument iDocument) {
	};

	public void onRecordBeforeRead(final ODocument iDocument) {
	};

	public void onRecordAfterRead(final ODocument iDocument) {
	};

	public void onRecordBeforeUpdate(final ODocument iDocument) {
	};

	public void onRecordAfterUpdate(final ODocument iDocument) {
	};

	public void onRecordBeforeDelete(final ODocument iDocument) {
	};

	public void onRecordAfterDelete(final ODocument iDocument) {
	};

	public void onTrigger(final TYPE iType, final ORecord<?> iRecord) {
		if (!(iRecord instanceof ODocument))
			return;

		ODocument document = (ODocument) iRecord;

		switch (iType) {
		case BEFORE_CREATE:
			onRecordBeforeCreate(document);
			break;
		case AFTER_CREATE:
			onRecordAfterCreate(document);
			break;
		case BEFORE_READ:
			onRecordBeforeRead(document);
			break;
		case AFTER_READ:
			onRecordAfterRead(document);
			break;
		case BEFORE_UPDATE:
			onRecordBeforeUpdate(document);
			break;
		case AFTER_UPDATE:
			onRecordAfterUpdate(document);
			break;
		case BEFORE_DELETE:
			onRecordBeforeDelete(document);
			break;
		case AFTER_DELETE:
			onRecordAfterDelete(document);
			break;
		}
	}
}

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

import com.orientechnologies.orient.core.db.ODatabase.STATUS;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Hook abstract class that calls separate methods for ODocument records.
 * 
 * @author Luca Garulli
 * @see ORecordHook
 */
public abstract class ODocumentHookAbstract implements ORecordHook {
	public boolean onRecordBeforeCreate(final ODocument iDocument) {
		return false;
	};

	public boolean onRecordAfterCreate(final ODocument iDocument) {
		return false;
	};

	public boolean onRecordBeforeRead(final ODocument iDocument) {
		return false;
	};

	public boolean onRecordAfterRead(final ODocument iDocument) {
		return false;
	};

	public boolean onRecordBeforeUpdate(final ODocument iDocument) {
		return false;
	};

	public boolean onRecordAfterUpdate(final ODocument iDocument) {
		return false;
	};

	public boolean onRecordBeforeDelete(final ODocument iDocument) {
		return false;
	};

	public boolean onRecordAfterDelete(final ODocument iDocument) {
		return false;
	};

	public boolean onTrigger(final TYPE iType, final ORecord<?> iRecord) {
		if (iRecord.getDatabase() != null && iRecord.getDatabase().getStatus() != STATUS.OPEN)
			return false;

		if (!(iRecord instanceof ODocument))
			return false;

		final ODocument document = (ODocument) iRecord;

		switch (iType) {
		case BEFORE_CREATE:
			return onRecordBeforeCreate(document);
		case AFTER_CREATE:
			return onRecordAfterCreate(document);
		case BEFORE_READ:
			return onRecordBeforeRead(document);
		case AFTER_READ:
			return onRecordAfterRead(document);
		case BEFORE_UPDATE:
			return onRecordBeforeUpdate(document);
		case AFTER_UPDATE:
			return onRecordAfterUpdate(document);
		case BEFORE_DELETE:
			return onRecordBeforeDelete(document);
		case AFTER_DELETE:
			return onRecordAfterDelete(document);
		}

		return false;
	}
}

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

/**
 * Hook abstract class that calls separate methods for each hook defined.
 * 
 * @author Luca Garulli
 * @see ORecordHook
 */
public abstract class ORecordHookAbstract implements ORecordHook {
	public boolean onRecordBeforeCreate(final ORecord<?> iRecord) {
		return false;
	};

	public boolean onRecordAfterCreate(final ORecord<?> iRecord) {
		return false;
	};

	public boolean onRecordBeforeRead(final ORecord<?> iRecord) {
		return false;
	};

	public boolean onRecordAfterRead(final ORecord<?> iRecord) {
		return false;
	};

	public boolean onRecordBeforeUpdate(final ORecord<?> iRecord) {
		return false;
	};

	public boolean onRecordAfterUpdate(final ORecord<?> iRecord) {
		return false;
	};

	public boolean onRecordBeforeDelete(final ORecord<?> iRecord) {
		return false;
	};

	public boolean onRecordAfterDelete(final ORecord<?> iRecord) {
		return false;
	};

	public boolean onTrigger(final TYPE iType, final ORecord<?> iRecord) {
		switch (iType) {
		case BEFORE_CREATE:
			return onRecordBeforeCreate(iRecord);
		case AFTER_CREATE:
			return onRecordAfterCreate(iRecord);
		case BEFORE_READ:
			return onRecordBeforeRead(iRecord);
		case AFTER_READ:
			return onRecordAfterRead(iRecord);
		case BEFORE_UPDATE:
			return onRecordBeforeUpdate(iRecord);
		case AFTER_UPDATE:
			return onRecordAfterUpdate(iRecord);
		case BEFORE_DELETE:
			return onRecordBeforeDelete(iRecord);
		case AFTER_DELETE:
			return onRecordAfterDelete(iRecord);
		}
		return false;
	}
}

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
	public abstract void onRecordBeforeCreate(ORecord<?> iRecord);

	public abstract void onRecordAfterCreate(ORecord<?> iRecord);

	public abstract void onRecordBeforeRead(ORecord<?> iRecord);

	public abstract void onRecordAfterRead(ORecord<?> iRecord);

	public abstract void onRecordBeforeUpdate(ORecord<?> iRecord);

	public abstract void onRecordAfterUpdate(ORecord<?> iRecord);

	public abstract void onRecordBeforeDelete(ORecord<?> iRecord);

	public abstract void onRecordAfterDelete(ORecord<?> iRecord);

	public void onTrigger(final TYPE iType, final ORecord<?> iRecord) {
		switch (iType) {
		case BEFORE_CREATE:
			onRecordBeforeCreate(iRecord);
			break;
		case AFTER_CREATE:
			onRecordAfterCreate(iRecord);
			break;
		case BEFORE_READ:
			onRecordBeforeRead(iRecord);
			break;
		case AFTER_READ:
			onRecordAfterRead(iRecord);
			break;
		case BEFORE_UPDATE:
			onRecordBeforeUpdate(iRecord);
			break;
		case AFTER_UPDATE:
			onRecordAfterUpdate(iRecord);
			break;
		case BEFORE_DELETE:
			onRecordBeforeDelete(iRecord);
			break;
		case AFTER_DELETE:
			onRecordAfterDelete(iRecord);
			break;
		}
	}
}

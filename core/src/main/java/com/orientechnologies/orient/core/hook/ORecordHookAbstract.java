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
	/**
	 * It's called just before to create the new iRecord.
	 * 
	 * @param iiRecord
	 *          The iRecord to create
	 * @return True if the iRecord has been modified and a new marshalling is required, otherwise false
	 */
	public boolean onRecordBeforeCreate(final ORecord<?> iiRecord) {
		return false;
	};

	/**
	 * It's called just after the iRecord is created.
	 * 
	 * @param iiRecord
	 *          The iRecord just created
	 */
	public void onRecordAfterCreate(final ORecord<?> iiRecord) {
	};

	/**
	 * It's called just before to read the iRecord.
	 * 
	 * @param iiRecord
	 *          The iRecord to read
	 */
	public void onRecordBeforeRead(final ORecord<?> iiRecord) {
	};

	/**
	 * It's called just after the iRecord is read.
	 * 
	 * @param iiRecord
	 *          The iRecord just read
	 */
	public void onRecordAfterRead(final ORecord<?> iiRecord) {
	};

	/**
	 * It's called just before to update the iRecord.
	 * 
	 * @param iiRecord
	 *          The iRecord to update
	 * @return True if the iRecord has been modified and a new marshalling is required, otherwise false
	 */
	public boolean onRecordBeforeUpdate(final ORecord<?> iiRecord) {
		return false;
	};

	/**
	 * It's called just after the iRecord is updated.
	 * 
	 * @param iiRecord
	 *          The iRecord just updated
	 */
	public void onRecordAfterUpdate(final ORecord<?> iiRecord) {
	};

	/**
	 * It's called just before to delete the iRecord.
	 * 
	 * @param iiRecord
	 *          The iRecord to delete
	 * @return True if the iRecord has been modified and a new marshalling is required, otherwise false
	 */
	public boolean onRecordBeforeDelete(final ORecord<?> iiRecord) {
		return false;
	};

	/**
	 * It's called just after the iRecord is deleted.
	 * 
	 * @param iiRecord
	 *          The iRecord just deleted
	 */
	public void onRecordAfterDelete(final ORecord<?> iiRecord) {
	};

	public boolean onTrigger(final TYPE iType, final ORecord<?> iRecord) {
		switch (iType) {
		case BEFORE_CREATE:
			return onRecordBeforeCreate(iRecord);

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
			return onRecordBeforeUpdate(iRecord);

		case AFTER_UPDATE:
			onRecordAfterUpdate(iRecord);
			break;

		case BEFORE_DELETE:
			return onRecordBeforeDelete(iRecord);

		case AFTER_DELETE:
			onRecordAfterDelete(iRecord);
			break;
		}
		return false;
	}
}

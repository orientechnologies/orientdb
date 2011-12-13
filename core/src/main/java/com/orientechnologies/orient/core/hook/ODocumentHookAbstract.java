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
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Hook abstract class that calls separate methods for ODocument records.
 * 
 * @author Luca Garulli
 * @see ORecordHook
 */
public abstract class ODocumentHookAbstract implements ORecordHook {
	/**
	 * It's called just before to create the new document.
	 * 
	 * @param iDocument
	 *          The document to create
	 * @return True if the document has been modified and a new marshalling is required, otherwise false
	 */
	public boolean onRecordBeforeCreate(final ODocument iDocument) {
		return false;
	};

	/**
	 * It's called just after the document is created.
	 * 
	 * @param iDocument
	 *          The document just created
	 */
	public void onRecordAfterCreate(final ODocument iDocument) {
	};

	/**
	 * It's called just before to read the document.
	 * 
	 * @param iDocument
	 *          The document to read
	 */
	public void onRecordBeforeRead(final ODocument iDocument) {
	};

	/**
	 * It's called just after the document is read.
	 * 
	 * @param iDocument
	 *          The document just read
	 */
	public void onRecordAfterRead(final ODocument iDocument) {
	};

	/**
	 * It's called just before to update the document.
	 * 
	 * @param iDocument
	 *          The document to update
	 * @return True if the document has been modified and a new marshalling is required, otherwise false
	 */
	public boolean onRecordBeforeUpdate(final ODocument iDocument) {
		return false;
	};

	/**
	 * It's called just after the document is updated.
	 * 
	 * @param iDocument
	 *          The document just updated
	 */
	public void onRecordAfterUpdate(final ODocument iDocument) {
	};

	/**
	 * It's called just before to delete the document.
	 * 
	 * @param iDocument
	 *          The document to delete
	 * @return True if the document has been modified and a new marshalling is required, otherwise false
	 */
	public boolean onRecordBeforeDelete(final ODocument iDocument) {
		return false;
	};

	/**
	 * It's called just after the document is deleted.
	 * 
	 * @param iDocument
	 *          The document just deleted
	 */
	public void onRecordAfterDelete(final ODocument iDocument) {
	};

	public boolean onTrigger(final TYPE iType, final ORecord<?> iRecord) {
		if (ODatabaseRecordThreadLocal.INSTANCE.isDefined() && ODatabaseRecordThreadLocal.INSTANCE.get().getStatus() != STATUS.OPEN)
			return false;

		if (!(iRecord instanceof ODocument))
			return false;

		final ODocument document = (ODocument) iRecord;

		switch (iType) {
		case BEFORE_CREATE:
			return onRecordBeforeCreate(document);

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
			return onRecordBeforeUpdate(document);

		case AFTER_UPDATE:
			onRecordAfterUpdate(document);
			break;

		case BEFORE_DELETE:
			return onRecordBeforeDelete(document);

		case AFTER_DELETE:
			onRecordAfterDelete(document);
			break;
		}

		return false;
	}
}

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
package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.db.ODatabasePoolAbstract;
import com.orientechnologies.orient.core.db.ODatabasePoolBase;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;

public class ODatabaseDocumentPool extends ODatabasePoolBase<ODatabaseDocumentTx> {

	private static ODatabaseDocumentPool	globalInstance	= new ODatabaseDocumentPool();

	@Override
	public void setup(final int iMinSize, final int iMaxSize) {
		if (dbPool == null)
			synchronized (this) {
				if (dbPool == null) {
					dbPool = new ODatabasePoolAbstract<ODatabaseDocumentTx>(this, iMinSize, iMaxSize) {

						public ODatabaseDocumentTx createNewResource(final String iDatabaseName, final Object... iAdditionalArgs) {
							if (iAdditionalArgs.length < 2)
								throw new OSecurityAccessException("Username and/or password missed");

							final ODatabaseDocumentTxPooled db = new ODatabaseDocumentTxPooled((ODatabaseDocumentPool) owner, iDatabaseName,
									(String) iAdditionalArgs[0], (String) iAdditionalArgs[1]);

							ODatabaseRecordThreadLocal.INSTANCE.set(db);

							return db;
						}

						public boolean reuseResource(final String iKey, final Object[] iAdditionalArgs, final ODatabaseDocumentTx iValue) {
							ODatabaseRecordThreadLocal.INSTANCE.set(iValue);

							if (!((ODatabaseDocumentTxPooled) iValue).isClosed()) {
								((ODatabaseDocumentTxPooled) iValue).reuse(owner, iAdditionalArgs);
								if (iValue.getStorage().isClosed())
									// STORAGE HAS BEEN CLOSED: REOPEN IT
									iValue.getStorage().open((String) iAdditionalArgs[0], (String) iAdditionalArgs[1], null);
								else if (!iValue.getUser().checkPassword((String) iAdditionalArgs[1]))
									throw new OSecurityAccessException(iValue.getName(), "User or password not valid for database: '"
											+ iValue.getName() + "'");

								return true;
							}
							return false;
						}
					};
				}
			}
	}

	public static ODatabaseDocumentPool global() {
		globalInstance.setup();
		return globalInstance;
	}
}

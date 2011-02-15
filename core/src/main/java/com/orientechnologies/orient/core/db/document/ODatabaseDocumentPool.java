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
import com.orientechnologies.orient.core.exception.OSecurityAccessException;

public class ODatabaseDocumentPool extends ODatabasePoolBase<ODatabaseDocumentTx> {

	private static ODatabaseDocumentPool	globalInstance	= new ODatabaseDocumentPool();

	@Override
	public void setup(final int iMinSize, final int iMaxSize) {
		if (dbPool == null)
			synchronized (this) {
				if (dbPool == null) {
					dbPool = new ODatabasePoolAbstract<ODatabaseDocumentTx>(this, iMinSize, iMaxSize) {

						public ODatabaseDocumentTx createNewResource(final String iDatabaseName, final String... iAdditionalArgs) {
							if (iAdditionalArgs.length < 2)
								throw new OSecurityAccessException("Username and/or password missed");

							return new ODatabaseDocumentTxPooled((ODatabaseDocumentPool) owner, iDatabaseName, iAdditionalArgs[0],
									iAdditionalArgs[1]);
						}

						@Override
						public ODatabaseDocumentTx reuseResource(String iKey, ODatabaseDocumentTx iValue) {
							((ODatabaseDocumentTxPooled) iValue).reuse(owner);
							if (iValue.getStorage().isClosed())
								// STORAGE HAS BEEN CLOSED: REOPEN IT
								iValue.getStorage().open(-1, iValue.getUser().getName(), iValue.getUser().getPassword(), null);
							return iValue;
						}
					};
				}
			}
	}

	public static ODatabaseDocumentPool global() {
		return globalInstance;
	}
}

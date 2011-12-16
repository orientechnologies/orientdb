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
package com.orientechnologies.orient.core.db.object;

import com.orientechnologies.orient.core.db.ODatabasePoolAbstract;
import com.orientechnologies.orient.core.db.ODatabasePoolBase;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OSecurityAccessException;

public class ODatabaseObjectPool extends ODatabasePoolBase<ODatabaseObjectTx> {
	private static ODatabaseObjectPool	globalInstance	= new ODatabaseObjectPool();

	@Override
	public void setup(final int iMinSize, final int iMaxSize) {
		if (dbPool == null)
			synchronized (this) {
				if (dbPool == null) {
					dbPool = new ODatabasePoolAbstract<ODatabaseObjectTx>(this, iMinSize, iMaxSize) {

						public ODatabaseObjectTx createNewResource(final String iDatabaseName, final Object... iAdditionalArgs) {
							if (iAdditionalArgs.length < 2)
								throw new OSecurityAccessException("Username and/or password missing");

							final ODatabaseObjectTxPooled db = new ODatabaseObjectTxPooled((ODatabaseObjectPool) owner, iDatabaseName,
									(String) iAdditionalArgs[0], (String) iAdditionalArgs[1]);

							ODatabaseRecordThreadLocal.INSTANCE.set(db.getUnderlying());

							return db;
						}

						public boolean reuseResource(final String iKey, final Object[] iAdditionalArgs, final ODatabaseObjectTx iValue) {
							ODatabaseRecordThreadLocal.INSTANCE.set(iValue.getUnderlying());

							((ODatabaseObjectTxPooled) iValue).reuse(owner, iAdditionalArgs);

							if (!iValue.getStorage().isClosed()) {
								iValue.getMetadata().getSecurity().authenticate((String) iAdditionalArgs[0], (String) iAdditionalArgs[1]);
								return true;
							}
							return false;
						}
					};
				}
			}
	}

	public static ODatabaseObjectPool global() {
		return globalInstance;
	}
}

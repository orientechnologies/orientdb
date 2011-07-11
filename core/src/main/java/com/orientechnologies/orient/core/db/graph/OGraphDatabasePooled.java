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
package com.orientechnologies.orient.core.db.graph;

import com.orientechnologies.orient.core.db.ODatabasePoolBase;
import com.orientechnologies.orient.core.db.ODatabasePooled;
import com.orientechnologies.orient.core.db.raw.ODatabaseRaw;
import com.orientechnologies.orient.core.exception.ODatabaseException;

/**
 * Pooled wrapper to the ODatabaseObjectTx class. Allows to being reused across calls. The close() method doesn't close the database
 * for real but release it to the owner pool. The database born as opened and will leave open until the pool is closed.
 * 
 * @author Luca Garulli
 * @see ODatabasePoolBase
 * 
 */
@SuppressWarnings("unchecked")
public class OGraphDatabasePooled extends OGraphDatabase implements ODatabasePooled {

	private OGraphDatabasePool	ownerPool;

	public OGraphDatabasePooled(final OGraphDatabasePool iOwnerPool, final String iURL, final String iUserName,
			final String iUserPassword) {
		super(iURL);
		ownerPool = iOwnerPool;
		super.open(iUserName, iUserPassword);
	}

	public void reuse(final Object iOwner) {
		ownerPool = (OGraphDatabasePool) iOwner;
		getMetadata().reload();
		checkForGraphSchema();
	}

	@Override
	public OGraphDatabasePooled open(String iUserName, String iUserPassword) {
		checkOpeness();
		if (!getUser().getName().equals(iUserName))
			throw new UnsupportedOperationException("Database instance was retrieved from a pool and has been used with the user '"
					+ getUser().getName() + "'. Get another database instance fro mthe pool with the right username and password");

		return this;
	}

	@Override
	public OGraphDatabasePooled create() {
		throw new UnsupportedOperationException(
				"Database instance was retrieved from a pool. You can't create the database in this way. Please use directly ODatabaseDocumentTx.create()");
	}

	@Override
	public boolean isClosed() {
		return ownerPool == null || super.isClosed();
	}

	/**
	 * Avoid to close it but rather release itself to the owner pool.
	 */
	@Override
	public void close() {
		if (isClosed())
			return;

		vertexBaseClass = null;
		edgeBaseClass = null;

		checkOpeness();
		rollback();

		getMetadata().close();
		((ODatabaseRaw) underlying.getUnderlying()).callOnCloseListeners();
		getLevel1Cache().clear();

		ownerPool.release(this);
		ownerPool = null;
	}

	public void forceClose() {
		super.close();
	}

	@Override
	protected void checkOpeness() {
		if (ownerPool == null)
			throw new ODatabaseException(
					"Database instance has been released to the pool. Get another database instance from the pool with the right username and password");

		super.checkOpeness();
	}

}

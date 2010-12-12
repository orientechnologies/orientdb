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
package com.orientechnologies.orient.core.db.object.jpa;

import javax.persistence.EntityTransaction;

import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.tx.OTransactionNoTx;

public class OJPAEntityTransaction implements EntityTransaction {
	private ODatabaseObjectTx	database;

	public OJPAEntityTransaction(final ODatabaseObjectTx iDatabase) {
		database = iDatabase;
	}

	public void begin() {
		database.getTransaction().begin();
	}

	public void commit() {
		database.getTransaction().commit();
	}

	public void rollback() {
		database.getTransaction().rollback();
	}

	public void setRollbackOnly() {
		throw new UnsupportedOperationException("merge");
	}

	public boolean getRollbackOnly() {
		return false;
	}

	public boolean isActive() {
		return !(database.getTransaction() instanceof OTransactionNoTx);
	}
}

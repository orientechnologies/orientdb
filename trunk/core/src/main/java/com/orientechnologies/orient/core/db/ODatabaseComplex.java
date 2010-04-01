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
package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;

public interface ODatabaseComplex<T extends Object> extends ODatabase, OUserObject2RecordHandler {

	public T load(final T iObject);

	public T load(final ORID iObjectId);

	public ODatabaseComplex<T> save(T iObject);

	public ODatabaseComplex<T> save(T iObject, String iClusterName);

	public ODatabaseComplex<T> delete(T iObject);

	public ODatabaseComplex<T> begin();

	public ODatabaseComplex<T> begin(TXTYPE iStatus);

	public ODatabaseComplex<T> commit();

	public ODatabaseComplex<T> rollback();

	public OMetadata getMetadata();

	public ODictionary<T> getDictionary();

	public ODatabaseComplex<?> getDatabaseOwner();

	public ODatabaseComplex<?> setDatabaseOwner(ODatabaseComplex<?> iOwner);

	public <DB extends ODatabase> DB getUnderlying();
}

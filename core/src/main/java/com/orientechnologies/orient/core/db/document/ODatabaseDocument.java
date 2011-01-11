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

import com.orientechnologies.orient.core.db.ODatabaseSchemaAware;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Generic interface for document based Database implementations.
 * 
 * @author Luca Garulli
 */
public interface ODatabaseDocument extends ODatabaseRecord, ODatabaseSchemaAware<ORecordInternal<?>> {

	/**
	 * Browses all the records of the specified class.
	 * 
	 * @param iClassName
	 *          Class name to iterate
	 * @return Iterator of ODocument instances
	 */
	public ORecordIteratorClass<ODocument> browseClass(String iClassName);
}

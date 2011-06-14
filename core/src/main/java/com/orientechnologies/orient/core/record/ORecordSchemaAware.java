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
package com.orientechnologies.orient.core.record;

import java.util.Set;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;

/**
 * Generic record representation with a schema definition. The object can be reused across call to the database.
 */
public interface ORecordSchemaAware<T> extends ORecordInternal<T> {

	public <RET> RET field(String iPropertyName);

	public ORecordSchemaAware<T> field(String iPropertyName, Object iValue);

	public <RET> RET field(String iPropertyName, OType iType);

	public ORecordSchemaAware<T> field(String iPropertyName, Object iPropertyValue, OType iType);

	public Object removeField(String iPropertyName);

	public Set<String> fieldNames();

	public Object[] fieldValues();

	public int size();

	public String getClassName();

	public void setClassName(String iClassName);

	public void setClassNameIfExists(String iClassName);

	public OClass getSchemaClass();

	public void validate() throws OValidationException;

	public ORecordSchemaAware<T> fill(ODatabaseRecord iDatabase, int iClassId, ORecordId iRid, int iVersion, byte[] iBuffer, boolean iDirty);
}

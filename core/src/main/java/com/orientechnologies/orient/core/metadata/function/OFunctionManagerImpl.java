/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
package com.orientechnologies.orient.core.metadata.function;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * Manages stored functions.
 * 
 * @author Luca Garulli
 * 
 */
public class OFunctionManagerImpl implements OFunctionManager {
  protected Map<String, ODocument> functions = new ConcurrentHashMap<String, ODocument>();

  public OFunctionManagerImpl() {
    // LOAD ALL THE FUNCTIONS IN MEMORY
    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.get();
    List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>("select from OFunction"));
    for (ODocument d : result)
      functions.put((String) d.field("name"), d);
  }

  public String[] getFunctionNames() {
    final String[] result = new String[functions.size()];
    return functions.keySet().toArray(result);
  }

  public ODocument getFunction(final String iName) {
    return functions.get(iName);
  }

  public synchronized ODocument createFunction(final String iName) {
    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.get();

    OClass functionClass = db.getMetadata().getSchema().getClass("OFunction");
    if (functionClass == null)
      functionClass = db.getMetadata().getSchema().createClass("OFunction");

    final ODocument f = new ODocument("OFunction").field("name", iName);
    functions.put(iName, f);

    return f;
  }
}

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

import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.script.OCommandExecutorFunction;
import com.orientechnologies.orient.core.command.script.OCommandFunction;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * Manages stored functions.
 * 
 * @author Luca Garulli
 * 
 */
public class OFunctionManagerImpl implements OFunctionManager {
  protected Map<String, OFunction> functions = new ConcurrentHashMap<String, OFunction>();

  static {
    OCommandManager.instance().registerExecutor(OCommandFunction.class, OCommandExecutorFunction.class);
  }

  public OFunctionManagerImpl() {
  }

  public void create() {
    init();
  }

  public void load() {
    // LOAD ALL THE FUNCTIONS IN MEMORY
    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.get();
    if (db.getMetadata().getSchema().existsClass("OFunction")) {
      List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>("select from OFunction"));
      for (ODocument d : result)
        functions.put((String) d.field("name"), new OFunction(d));
    }
  }

  public String[] getFunctionNames() {
    final String[] result = new String[functions.size()];
    return functions.keySet().toArray(result);
  }

  public OFunction getFunction(final String iName) {
    return functions.get(iName);
  }

  public synchronized OFunction createFunction(final String iName) {
    init();

    final OFunction f = new OFunction().setName(iName);
    functions.put(iName, f);

    return f;
  }

  public void close() {
    functions.clear();
  }

  protected void init() {
    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.get();
    if (db.getMetadata().getSchema().existsClass("OFunction"))
      return;

    final OClass f = db.getMetadata().getSchema().createClass("OFunction");
    f.createProperty("name", OType.STRING);
    f.createProperty("code", OType.STRING);
    f.createProperty("language", OType.STRING);
    f.createProperty("parameters", OType.EMBEDDEDLIST, OType.STRING);
  }
}

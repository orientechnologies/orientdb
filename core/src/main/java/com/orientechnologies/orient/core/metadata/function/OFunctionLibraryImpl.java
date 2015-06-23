/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.metadata.function;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.script.OCommandExecutorFunction;
import com.orientechnologies.orient.core.command.script.OCommandFunction;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassImpl;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

/**
 * Manages stored functions.
 * 
 * @author Luca Garulli
 */
public class OFunctionLibraryImpl implements OFunctionLibrary {
  protected Map<String, OFunction> functions = new ConcurrentHashMap<String, OFunction>();

  static {
    OCommandManager.instance().registerExecutor(OCommandFunction.class, OCommandExecutorFunction.class);
  }

  public OFunctionLibraryImpl() {
  }

  public void create() {
    init();
  }

  public void load() {
    functions.clear();

    // LOAD ALL THE FUNCTIONS IN MEMORY
    final ODatabaseDocument db = ODatabaseRecordThreadLocal.INSTANCE.get();
    if (((OMetadataInternal) db.getMetadata()).getImmutableSchemaSnapshot().existsClass("OFunction")) {
      List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>("select from OFunction order by name"));
      for (ODocument d : result) {
        d.reload();
        functions.put(d.field("name").toString().toUpperCase(), new OFunction(d));
      }
    }
  }

  public Set<String> getFunctionNames() {
    return Collections.unmodifiableSet(functions.keySet());
  }

  public OFunction getFunction(final String iName) {
    return functions.get(iName.toUpperCase());
  }

  public synchronized OFunction createFunction(final String iName) {
    init();

    final OFunction f = new OFunction().setName(iName);
    f.save();
    functions.put(iName.toUpperCase(), f);

    return f;
  }

  public void close() {
    functions.clear();
  }

  protected void init() {
    final ODatabaseDocument db = ODatabaseRecordThreadLocal.INSTANCE.get();
    if (db.getMetadata().getSchema().existsClass("OFunction"))
      return;

    final OClassImpl f = (OClassImpl) db.getMetadata().getSchema().createClass("OFunction");
    f.createProperty("name", OType.STRING, (OType) null, false);
    f.createProperty("code", OType.STRING, (OType) null, false);
    f.createProperty("language", OType.STRING, (OType) null, false);
    f.createProperty("idempotent", OType.BOOLEAN, (OType) null, false);
    f.createProperty("parameters", OType.EMBEDDEDLIST, OType.STRING, false);
  }
}

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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.script.OCommandExecutorFunction;
import com.orientechnologies.orient.core.command.script.OCommandFunction;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
    // COPY CALLBACK IN RAM
    final Map<String, OCallable<Object, Map<Object, Object>>> callbacks = new HashMap<String, OCallable<Object, Map<Object, Object>>>();
    for (Map.Entry<String, OFunction> entry : functions.entrySet()) {
      if (entry.getValue().getCallback() != null)
        callbacks.put(entry.getKey(), entry.getValue().getCallback());
    }

    functions.clear();

    // LOAD ALL THE FUNCTIONS IN MEMORY
    final ODatabaseDocument db = ODatabaseRecordThreadLocal.INSTANCE.get();
    if (((OMetadataInternal) db.getMetadata()).getImmutableSchemaSnapshot().existsClass("OFunction")) {
      List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>("select from OFunction order by name"));
      for (ODocument d : result) {
        d.reload();
        final OFunction f = new OFunction(d);

        // RESTORE CALLBACK IF ANY
        f.setCallback(callbacks.get(f.getName()));

        functions.put(d.field("name").toString().toUpperCase(), f);
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
    try {
      f.save();
    } catch (ORecordDuplicatedException ex) {
      throw OException.wrapException(new OFunctionDuplicatedException("Function with name '" + iName + "' already exist"), null);
    }
    functions.put(iName.toUpperCase(), f);

    return f;
  }

  public void close() {
    functions.clear();
  }

  protected void init() {
    final ODatabaseDocument db = ODatabaseRecordThreadLocal.INSTANCE.get();
    if (db.getMetadata().getSchema().existsClass("OFunction")) {
      final OClass f = db.getMetadata().getSchema().getClass("OFunction");
      OProperty prop = f.getProperty("name");
      if (prop.getAllIndexes().isEmpty())
        prop.createIndex(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX);
      return;
    }

    final OClass f = db.getMetadata().getSchema().createClass("OFunction");
    OProperty prop = f.createProperty("name", OType.STRING, (OType) null, true);
    prop.set(OProperty.ATTRIBUTES.NOTNULL, true);
    prop.set(OProperty.ATTRIBUTES.MANDATORY, true);
    prop.createIndex(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX);
    f.createProperty("code", OType.STRING, (OType) null, true);
    f.createProperty("language", OType.STRING, (OType) null, true);
    f.createProperty("idempotent", OType.BOOLEAN, (OType) null, true);
    f.createProperty("parameters", OType.EMBEDDEDLIST, OType.STRING, true);
  }

  @Override
  public synchronized void dropFunction(OFunction function) {
    String name = function.getName();
    ODocument doc = function.getDocument();
    doc.delete();
    functions.remove(name.toUpperCase());
  }

  @Override
  public synchronized void dropFunction(String iName) {
    OFunction function = getFunction(iName);
    ODocument doc = function.getDocument();
    doc.delete();
    functions.remove(iName.toUpperCase());
  }
}

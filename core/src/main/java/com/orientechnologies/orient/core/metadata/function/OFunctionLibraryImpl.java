/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.metadata.function;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OMetadataUpdateListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * Manages stored functions.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OFunctionLibraryImpl {
  public static final String CLASSNAME = "OFunction";
  protected final Map<String, OFunction> functions = new ConcurrentHashMap<String, OFunction>();
  private AtomicBoolean needReload = new AtomicBoolean(false);

  public OFunctionLibraryImpl() {}

  public void create(ODatabaseDocumentInternal db) {
    init(db);
  }

  public void load() {
    throw new UnsupportedOperationException();
  }

  public void load(ODatabaseDocumentInternal db) {
    // COPY CALLBACK IN RAM
    final Map<String, OCallable<Object, Map<Object, Object>>> callbacks =
        new HashMap<String, OCallable<Object, Map<Object, Object>>>();
    for (Map.Entry<String, OFunction> entry : functions.entrySet()) {
      if (entry.getValue().getCallback() != null)
        callbacks.put(entry.getKey(), entry.getValue().getCallback());
    }

    functions.clear();

    // LOAD ALL THE FUNCTIONS IN MEMORY
    if (db.getMetadata().getImmutableSchemaSnapshot().existsClass("OFunction")) {
      try (OResultSet result = db.query("select from OFunction order by name")) {
        while (result.hasNext()) {
          OResult res = result.next();
          ODocument d = (ODocument) res.getElement().get();
          // skip the function records which do not contain real data
          if (d.fields() == 0) continue;

          final OFunction f = new OFunction(d);

          // RESTORE CALLBACK IF ANY
          f.setCallback(callbacks.get(f.getName()));

          functions.put(d.field("name").toString().toUpperCase(Locale.ENGLISH), f);
        }
      }
    }
  }

  public void droppedFunction(ODocument function) {
    functions.remove(function.field("name").toString());
    onFunctionsChanged(ODatabaseRecordThreadLocal.instance().get());
  }

  public void createdFunction(ODocument function) {
    ODocument metadataCopy = function.copy();
    final OFunction f = new OFunction(metadataCopy);
    functions.put(metadataCopy.field("name").toString().toUpperCase(Locale.ENGLISH), f);
    onFunctionsChanged(ODatabaseRecordThreadLocal.instance().get());
  }

  public Set<String> getFunctionNames() {
    return Collections.unmodifiableSet(functions.keySet());
  }

  public OFunction getFunction(final String iName) {
    reloadIfNeeded(ODatabaseRecordThreadLocal.instance().get());
    return functions.get(iName.toUpperCase(Locale.ENGLISH));
  }

  public OFunction createFunction(final String iName) {
    throw new UnsupportedOperationException("Use Create function with database on internal api");
  }

  public synchronized OFunction createFunction(
      ODatabaseDocumentInternal database, final String iName) {
    init(database);
    reloadIfNeeded(ODatabaseRecordThreadLocal.instance().get());

    final OFunction f = new OFunction().setName(iName);
    try {
      f.save();
    } catch (ORecordDuplicatedException ex) {
      OLogManager.instance().error(this, "Exception is suppressed, original exception is ", ex);

      //noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException
      throw OException.wrapException(
          new OFunctionDuplicatedException("Function with name '" + iName + "' already exist"),
          null);
    }
    functions.put(iName.toUpperCase(Locale.ENGLISH), f);
    return f;
  }

  public void close() {
    functions.clear();
  }

  protected void init(final ODatabaseDocument db) {
    if (db.getMetadata().getSchema().existsClass("OFunction")) {
      final OClass f = db.getMetadata().getSchema().getClass("OFunction");
      OProperty prop = f.getProperty("name");
      if (prop.getAllIndexes().isEmpty()) prop.createIndex(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX);
      return;
    }

    final OClass f = db.getMetadata().getSchema().createClass("OFunction");
    OProperty prop = f.createProperty("name", OType.STRING, (OType) null, true);
    prop.createIndex(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX);
    f.createProperty("code", OType.STRING, (OType) null, true);
    f.createProperty("language", OType.STRING, (OType) null, true);
    f.createProperty("idempotent", OType.BOOLEAN, (OType) null, true);
    f.createProperty("parameters", OType.EMBEDDEDLIST, OType.STRING, true);
  }

  public synchronized void dropFunction(OFunction function) {
    reloadIfNeeded(ODatabaseRecordThreadLocal.instance().get());
    String name = function.getName();
    ODocument doc = function.getDocument();
    doc.delete();
    functions.remove(name.toUpperCase(Locale.ENGLISH));
  }

  public synchronized void dropFunction(String iName) {
    reloadIfNeeded(ODatabaseRecordThreadLocal.instance().get());
    OFunction function = getFunction(iName);
    ODocument doc = function.getDocument();
    doc.delete();
    functions.remove(iName.toUpperCase(Locale.ENGLISH));
  }

  public void updatedFunction(ODocument function) {
    ODatabaseDocumentInternal database = ODatabaseRecordThreadLocal.instance().get();
    reloadIfNeeded(database);
    String oldName = (String) function.getOriginalValue("name");
    if (oldName != null) {
      functions.remove(oldName.toUpperCase(Locale.ENGLISH));
    }
    ODocument metadataCopy = function.copy();
    OCallable<Object, Map<Object, Object>> callBack = null;
    OFunction oldFunction = functions.get(metadataCopy.field("name").toString());
    if (oldFunction != null) {
      callBack = oldFunction.getCallback();
    }
    final OFunction f = new OFunction(metadataCopy);
    if (callBack != null) {
      f.setCallback(callBack);
    }
    functions.put(metadataCopy.field("name").toString().toUpperCase(Locale.ENGLISH), f);
    onFunctionsChanged(database);
  }

  private void reloadIfNeeded(ODatabaseDocumentInternal database) {
    if (needReload.get()) {
      load(database);
      needReload.set(false);
    }
  }

  private void onFunctionsChanged(ODatabaseDocumentInternal database) {
    for (OMetadataUpdateListener listener : database.getSharedContext().browseListeners()) {
      listener.onFunctionLibraryUpdate(database.getName());
    }
    database.getSharedContext().getOrientDB().getScriptManager().close(database.getName());
  }

  public synchronized void update() {
    needReload.set(true);
  }

  public static void validateFunctionRecord(ODocument doc) throws ODatabaseException {
    String name = doc.getProperty("name");
    if (!Pattern.compile("[A-Za-z][A-Za-z0-9_]*").matcher(name).matches()) {
      throw new ODatabaseException("Invalid function name: " + name);
    }
  }
}

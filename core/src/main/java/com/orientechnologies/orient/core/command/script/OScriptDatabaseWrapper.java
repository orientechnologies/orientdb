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
package com.orientechnologies.orient.core.command.script;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Map;

/**
 * Database wrapper class to use from scripts.
 *
 * @author Enrico Risa (e.risa--(at)--orientdb.com)
 */
public class OScriptDatabaseWrapper {
  protected ODatabaseDocumentInternal database;

  public OScriptDatabaseWrapper(final ODatabaseDocumentInternal database) {
    this.database = database;
  }

  public OResultSet query(final String iText, final Object... iParameters) {
    return this.database.query(iText, iParameters);
  }

  public OResultSet query(final String query, Map<String, Object> iParameters) {
    return this.database.query(query, iParameters);
  }

  public OResultSet command(final String iText, final Object... iParameters) {
    return this.database.command(iText, iParameters);
  }

  public OResultSet command(final String query, Map<String, Object> iParameters) {
    return this.database.query(query, iParameters);
  }

  public OResultSet execute(String language, final String script, final Object... iParameters) {
    return this.database.execute(language, script, iParameters);
  }

  public OResultSet execute(String language, final String script, Map<String, Object> iParameters) {
    return this.database.execute(language, script, iParameters);
  }

  public OElement newInstance() {
    return this.database.newInstance();
  }

  public OElement newInstance(String className) {
    return this.database.newInstance(className);
  }

  public OVertex newVertex() {
    return this.database.newVertex();
  }

  public OVertex newVertex(String className) {
    return this.database.newVertex(className);
  }

  public OEdge newEdge(OVertex from, OVertex to) {
    return this.database.newEdge(from, to);
  }

  public OEdge newEdge(OVertex from, OVertex to, String edgeClassName) {
    return this.database.newEdge(from, to, edgeClassName);
  }

  public ORecord save(ORecord element) {
    return this.database.save(element);
  }

  public void delete(ORecord record) {
    this.database.delete(record);
  }

  public OBlob newBlob() {
    return this.database.newBlob();
  }
}

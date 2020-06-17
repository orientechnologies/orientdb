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

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OConfigurationException;

/**
 * Orient wrapper class to use from scripts.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
@Deprecated
public class OScriptOrientWrapper {
  protected final ODatabase db;

  public OScriptOrientWrapper() {
    this.db = null;
  }

  public OScriptOrientWrapper(final ODatabase db) {
    this.db = db;
  }

  public OScriptDocumentDatabaseWrapper getDatabase() {
    if (db == null) throw new OConfigurationException("No database instance found in context");

    if (db instanceof ODatabaseDocumentInternal)
      return new OScriptDocumentDatabaseWrapper((ODatabaseDocumentInternal) db);

    throw new OConfigurationException(
        "No valid database instance found in context: " + db + ", class: " + db.getClass());
  }
}

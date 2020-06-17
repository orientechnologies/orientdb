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

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionFactory;
import java.util.Set;

/**
 * Dynamic function factory bound to the database's functions
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ODatabaseFunctionFactory implements OSQLFunctionFactory {
  @Override
  public boolean hasFunction(final String iName) {
    final ODatabaseDocument db = ODatabaseRecordThreadLocal.instance().get();
    return db.getMetadata().getFunctionLibrary().getFunction(iName) != null;
  }

  @Override
  public Set<String> getFunctionNames() {
    final ODatabaseDocument db = ODatabaseRecordThreadLocal.instance().get();
    return db.getMetadata().getFunctionLibrary().getFunctionNames();
  }

  @Override
  public OSQLFunction createFunction(final String name) throws OCommandExecutionException {
    final ODatabaseDocument db = ODatabaseRecordThreadLocal.instance().get();
    final OFunction f = db.getMetadata().getFunctionLibrary().getFunction(name);
    return new ODatabaseFunction(f);
  }
}

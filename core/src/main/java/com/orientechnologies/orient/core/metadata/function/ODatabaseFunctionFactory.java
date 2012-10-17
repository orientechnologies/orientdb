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

import java.util.Set;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionFactory;

/**
 * Dynamic function factory bound to the database's functions
 * 
 * @author Luca Garulli
 * 
 */
public class ODatabaseFunctionFactory implements OSQLFunctionFactory {
  @Override
  public boolean hasFunction(final String iName) {
    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.get();
    return db.getMetadata().getFunctionLibrary().getFunction(iName) != null;
  }

  @Override
  public Set<String> getFunctionNames() {
    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.get();
    return db.getMetadata().getFunctionLibrary().getFunctionNames();
  }

  @Override
  public OSQLFunction createFunction(final String name) throws OCommandExecutionException {
    final ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.get();
    final OFunction f = db.getMetadata().getFunctionLibrary().getFunction(name);
    return new ODatabaseFunction(f);
  }
}

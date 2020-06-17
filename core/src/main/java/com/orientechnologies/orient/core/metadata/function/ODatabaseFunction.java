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

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.functions.OSQLFunction;
import java.util.List;

/**
 * Dynamic function factory bound to the database's functions
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class ODatabaseFunction implements OSQLFunction {
  private final OFunction f;

  public ODatabaseFunction(final OFunction f) {
    this.f = f;
  }

  @Override
  public Object execute(
      Object iThis,
      final OIdentifiable iCurrentRecord,
      Object iCurrentResult,
      final Object[] iFuncParams,
      final OCommandContext iContext) {
    return f.executeInContext(iContext, iFuncParams);
  }

  @Override
  public boolean aggregateResults() {
    return false;
  }

  @Override
  public boolean filterResult() {
    return false;
  }

  @Override
  public String getName() {
    return f.getName();
  }

  @Override
  public int getMinParams() {
    return 0;
  }

  @Override
  public int getMaxParams() {
    return f.getParameters() != null ? f.getParameters().size() : 0;
  }

  @Override
  public String getSyntax() {
    final StringBuilder buffer = new StringBuilder(512);
    buffer.append(f.getName());
    buffer.append('(');
    final List<String> params = f.getParameters();
    for (int p = 0; p < params.size(); ++p) {
      if (p > 0) buffer.append(',');
      buffer.append(params.get(p));
    }
    buffer.append(')');
    return buffer.toString();
  }

  @Override
  public Object getResult() {
    return null;
  }

  @Override
  public void setResult(final Object iResult) {}

  @Override
  public void config(final Object[] configuredParameters) {}

  @Override
  public boolean shouldMergeDistributedResult() {
    return false;
  }

  @Override
  public Object mergeDistributedResult(List<Object> resultsToMerge) {
    return null;
  }
}

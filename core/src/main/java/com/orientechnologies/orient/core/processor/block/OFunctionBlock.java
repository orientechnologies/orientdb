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
package com.orientechnologies.orient.core.processor.block;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.processor.OComposableProcessor;
import com.orientechnologies.orient.core.processor.OProcessException;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OFunctionBlock extends OAbstractBlock {
  @SuppressWarnings("unchecked")
  @Override
  public Object processBlock(OComposableProcessor iManager, final ODocument iConfig, final OCommandContext iContext,
      final boolean iReadOnly) {
    final String function = getRequiredFieldOfClass(iConfig, "function", String.class);

    final Object[] args;
    final Collection<Object> configuredArgs = getFieldOfClass(iConfig, "args", Collection.class);
    if (configuredArgs != null) {
      args = new Object[configuredArgs.size()];
      int argIdx = 0;
      for (Object arg : configuredArgs) {
        Object value = resolveInContext(arg, iContext);

        if (value instanceof List<?>)
          // RHINO DOESN'T TREAT LIST AS ARRAY: CONVERT IT
          value = ((List<?>) value).toArray();

        args[argIdx++] = value;
      }
    } else
      args = null;

    final OFunction f = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getFunctionLibrary().getFunction(function);
    if (f == null)
      throw new OProcessException("Function '" + function + "' was not found");

    debug(iContext, "Calling: " + function + "(" + Arrays.toString(args) + ")...");

    final Object result = f.executeInContext(iContext, args);

    debug(iContext, "<- Returned " + result);

    final String ret = getField(iConfig, "return");
    assignVariable(iContext, ret, result);

    return null;
  }

  @Override
  public String getName() {
    return "function";
  }
}
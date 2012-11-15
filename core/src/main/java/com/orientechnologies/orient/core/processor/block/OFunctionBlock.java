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
  public Object processBlock(OComposableProcessor iManager, final OCommandContext iContext, final ODocument iConfig,
      ODocument iOutput, final boolean iReadOnly) {
    final String function = getRequiredFieldOfClass(iContext, iConfig, "function", String.class);

    final Object[] args;
    final Collection<Object> configuredArgs = getFieldOfClass(iContext, iConfig, "args", Collection.class);
    if (configuredArgs != null) {
      args = new Object[configuredArgs.size()];
      int argIdx = 0;
      for (Object arg : configuredArgs) {
        Object value = resolveValue(iContext, arg);

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

    return f.executeInContext(iContext, args);
  }

  @Override
  public String getName() {
    return "function";
  }
}
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.processor.OComposableProcessor;
import com.orientechnologies.orient.core.processor.OProcessException;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OExecuteBlock extends OAbstractBlock {
  @Override
  public Object processBlock(OComposableProcessor iManager, final OCommandContext iContext, final ODocument iConfig,
      ODocument iOutput, final boolean iReadOnly) {

    final ODocument foreach = getFieldOfClass(iContext, iConfig, "foreach", ODocument.class);
    final Object doClause = getRequiredField(iContext, iConfig, "do");
    String returnType = (String) getFieldOfClass(iContext, iConfig, "returnType", String.class);

    Object returnValue = null;
    if (returnType == null)
      returnType = "last";
    else if ("list".equalsIgnoreCase(returnType))
      returnValue = new ArrayList<Object>();
    else if ("set".equalsIgnoreCase(returnType))
      returnValue = new HashSet<Object>();

    int iterated = 0;

    if (foreach != null) {
      final Object result = delegate("foreach", iManager, (ODocument) foreach, iContext, iOutput, iReadOnly);
      if (!OMultiValue.isIterable(result))
        throw new OProcessException("Result of 'foreach' block (" + foreach + ") must be iterable but found " + result.getClass());

      for (Object current : OMultiValue.getMultiValueIterable(result)) {
        if (current instanceof Map.Entry)
          current = ((Entry<?, ?>) current).getValue();

        assignVariable(iContext, "current", current);

        debug(iContext, "Executing...");
        returnValue = executeDo(iManager, iContext, doClause, returnType, returnValue, iOutput, iReadOnly);
        debug(iContext, "Done");

        iterated++;
      }

    } else {
      debug(iContext, "Executing...");
      returnValue = executeDo(iManager, iContext, doClause, returnType, returnValue, iOutput, iReadOnly);
      debug(iContext, "Done");
    }

    debug(iContext, "Executed %d iteration and returned type %s", iterated, returnType);
    return returnValue;
  }

  private Object executeDo(OComposableProcessor iManager, final OCommandContext iContext, final Object iDoClause,
      final String returnType, Object returnValue, ODocument iOutput, final boolean iReadOnly) {
    int i = 0;

    if (isBlock(iDoClause)) {
      returnValue = executeBlock(iManager, iContext, "do", iDoClause, iOutput, iReadOnly, returnType, returnValue);
    } else
      for (Object item : OMultiValue.getMultiValueIterable(iDoClause)) {
        final String blockId = "do[" + i + "]";

        returnValue = executeBlock(iManager, iContext, blockId, item, iOutput, iReadOnly, returnType, returnValue);

        ++i;
      }

    return returnValue;
  }

  @SuppressWarnings("unchecked")
  private Object executeBlock(OComposableProcessor iManager, final OCommandContext iContext, final String iName,
      final Object iValue, ODocument iOutput, final boolean iReadOnly, final String returnType, Object returnValue) {
    Object result;
    if (isBlock(iValue))
      // EXECUTE SINGLE BLOCK
      result = delegate(iName, iManager, (ODocument) iValue, iContext, iOutput, iReadOnly);
    else {
      // EXECUTE ENTIRE PROCESS
      try {
        result = iManager.processFromFile(iName, iContext, iReadOnly);
      } catch (Exception e) {
        throw new OProcessException("Error on processing '" + iName + "' field of '" + getName() + "' block", e);
      }
    }

    if ("last".equalsIgnoreCase(returnType))
      returnValue = result;
    else if ("list".equalsIgnoreCase(returnType) || "set".equalsIgnoreCase(returnType))
      ((Collection<Object>) returnValue).add(result);

    return returnValue;
  }

  @Override
  public String getName() {
    return "execute";
  }
}
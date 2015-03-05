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
  public static final String NAME = "execute";
  private Object             returnValue;

  @Override
  public Object processBlock(OComposableProcessor iManager, final OCommandContext iContext, final ODocument iConfig,
      ODocument iOutput, final boolean iReadOnly) {

    final Object foreach = getField(iContext, iConfig, "foreach", false);
    String returnType = (String) getFieldOfClass(iContext, iConfig, "returnType", String.class);

    returnValue = null;
    if (returnType == null) {
        returnType = "last";
    } else if ("list".equalsIgnoreCase(returnType)) {
        returnValue = new ArrayList<Object>();
    } else if ("set".equalsIgnoreCase(returnType)) {
        returnValue = new HashSet<Object>();
    }

    int iterated = 0;

    final Object beginClause = getField(iContext, iConfig, "begin");
    if (beginClause != null) {
        executeBlock(iManager, iContext, "begin", beginClause, iOutput, iReadOnly, returnType, returnValue);
    }

    if (foreach != null) {
      Object result;
      if (foreach instanceof ODocument) {
          result = delegate("foreach", iManager, (ODocument) foreach, iContext, iOutput, iReadOnly);
      } else if (foreach instanceof Map) {
          result = ((Map<?, ?>) foreach).values();
      } else {
          result = foreach;
      }

      if (!OMultiValue.isIterable(result)) {
          throw new OProcessException("Result of 'foreach' block (" + foreach + ") must be iterable but found " + result.getClass());
      }

      for (Object current : OMultiValue.getMultiValueIterable(result)) {
        if (current instanceof Map.Entry) {
            current = ((Entry<?, ?>) current).getValue();
        }

        assignVariable(iContext, "current", current);
        assignVariable(iContext, "currentIndex", iterated);

        debug(iContext, "Executing...");
        final Object doClause = getRequiredField(iContext, iConfig, "do");

        returnValue = executeDo(iManager, iContext, doClause, returnType, returnValue, iOutput, iReadOnly);

        debug(iContext, "Done");

        iterated++;
      }

    } else {
      debug(iContext, "Executing...");
      final Object doClause = getRequiredField(iContext, iConfig, "do");
      returnValue = executeDo(iManager, iContext, doClause, returnType, returnValue, iOutput, iReadOnly);
      debug(iContext, "Done");
    }

    final Object endClause = getField(iContext, iConfig, "end");
    if (endClause != null) {
        executeBlock(iManager, iContext, "end", endClause, iOutput, iReadOnly, returnType, returnValue);
    }

    debug(iContext, "Executed %d iteration and returned type %s", iterated, returnType);
    return returnValue;
  }

  private Object executeDo(OComposableProcessor iManager, final OCommandContext iContext, final Object iDoClause,
      final String returnType, Object returnValue, ODocument iOutput, final boolean iReadOnly) {
    int i = 0;

    if (isBlock(iDoClause)) {
      returnValue = executeBlock(iManager, iContext, "do", iDoClause, iOutput, iReadOnly, returnType, returnValue);
    } else {
        for (Object item : OMultiValue.getMultiValueIterable(iDoClause)) {
            final String blockId = "do[" + i + "]";
            
            returnValue = executeBlock(iManager, iContext, blockId, item, iOutput, iReadOnly, returnType, returnValue);
            
            ++i;
        }
    }

    return returnValue;
  }

  @SuppressWarnings("unchecked")
  private Object executeBlock(OComposableProcessor iManager, final OCommandContext iContext, final String iName,
      final Object iValue, ODocument iOutput, final boolean iReadOnly, final String returnType, Object returnValue) {

    Boolean merge = iValue instanceof ODocument ? getFieldOfClass(iContext, (ODocument) iValue, "merge", Boolean.class)
        : Boolean.FALSE;
    if (merge == null) {
        merge = Boolean.FALSE;
    }

    Object result;
    if (isBlock(iValue)) {
      // EXECUTE SINGLE BLOCK
      final ODocument value = (ODocument) iValue;
      result = delegate(iName, iManager, value, iContext, iOutput, iReadOnly);
      if (value.containsField("return")) {
          return returnValue;
      }
    } else {
      // EXECUTE ENTIRE PROCESS
      try {
        result = iManager.processFromFile(iName, iContext, iReadOnly);
      } catch (Exception e) {
        throw new OProcessException("Error on processing '" + iName + "' field of '" + getName() + "' block", e);
      }
    }

    if ("last".equalsIgnoreCase(returnType)) {
        returnValue = result;
    } else if (result != null && ("list".equalsIgnoreCase(returnType) || "set".equalsIgnoreCase(returnType))) {
      if (result instanceof Collection<?> && merge) {
        debug(iContext, "Merging content of collection with size %d with the master with size %d",
            ((Collection<? extends Object>) result).size(), ((Collection<? extends Object>) returnValue).size());
        ((Collection<Object>) returnValue).addAll((Collection<? extends Object>) result);
      } else {
          ((Collection<Object>) returnValue).add(result);
        }
    }

    return returnValue;
  }

  @Override
  public String getName() {
    return NAME;
  }

  public Object getReturnValue() {
    return returnValue;
  }

  public void setReturnValue(Object returnValue) {
    this.returnValue = returnValue;
  }
}

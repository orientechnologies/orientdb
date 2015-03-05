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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.processor.OComposableProcessor;
import com.orientechnologies.orient.core.processor.OProcessException;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class OFunctionBlock extends OAbstractBlock {
  public static final String NAME = "function";

  @SuppressWarnings("unchecked")
  @Override
  public Object processBlock(OComposableProcessor iManager, final OCommandContext iContext, final ODocument iConfig,
      ODocument iOutput, final boolean iReadOnly) {
    final String function = getRequiredFieldOfClass(iContext, iConfig, NAME, String.class);

    final Object[] args;
    final Collection<Object> configuredArgs = getFieldOfClass(iContext, iConfig, "args", Collection.class);
    if (configuredArgs != null) {
      args = new Object[configuredArgs.size()];
      int argIdx = 0;
      for (Object arg : configuredArgs) {
        Object value = resolveValue(iContext, arg, true);

        if (value instanceof List<?>) {
            // RHINO DOESN'T TREAT LIST AS ARRAY: CONVERT IT
            value = ((List<?>) value).toArray();
        }

        args[argIdx++] = value;
      }
    } else {
        args = null;
    }

    final OFunction f = ODatabaseRecordThreadLocal.INSTANCE.get().getMetadata().getFunctionLibrary().getFunction(function);
    if (f != null) {
      debug(iContext, "Calling database function: " + function + "(" + Arrays.toString(args) + ")...");
      return f.executeInContext(iContext, args);
    }

    int lastDot = function.lastIndexOf('.');
    if (lastDot > -1) {
      final String clsName = function.substring(0, lastDot);
      final String methodName = function.substring(lastDot + 1);
      Class<?> cls = null;
      try {
        cls = Class.forName(clsName);

        Class<?>[] argTypes = new Class<?>[args.length];
        for (int i = 0; i < args.length; ++i) {
            argTypes[i] = args[i] == null ? null : args[i].getClass();
        }

        Method m = cls.getMethod(methodName, argTypes);

        debug(iContext, "Calling Java function: " + m + "(" + Arrays.toString(args).replace("%", "%%") + ")...");
        return m.invoke(null, args);

      } catch (NoSuchMethodException e) {

        for (Method m : cls.getMethods()) {
          if (m.getName().equals(methodName) && m.getParameterTypes().length == args.length) {
            try {
              debug(iContext, "Calling Java function: " + m + "(" + Arrays.toString(args) + ")...");
              return m.invoke(null, args);
            } catch (IllegalArgumentException e1) {
              // DO NOTHING, LOOK FOR ANOTHER METHOD
            } catch (Exception e1) {
              OLogManager.instance().error(this, "Error on calling function '%s'", e, function);
              throw new OProcessException("Error on calling function '" + function + "'", e);
            }
          }
        }

        // METHOD NOT FOUND!
        debug(iContext, "Method not found: " + clsName + "." + methodName + "(" + Arrays.toString(args) + ")");

        for (Method m : cls.getMethods()) {
          final StringBuilder candidates = new StringBuilder();
          if (m.getName().equals(methodName)) {
            candidates.append("-" + m + "\n");
          }
          if (candidates.length() > 0) {
              debug(iContext, "Candidate methods were: \n" + candidates);
          } else {
              debug(iContext, "No candidate methods were found");
          }
        }

      } catch (ClassNotFoundException e) {
        throw new OProcessException("Function '" + function + "' was not found because the class '" + clsName + "' was not found");
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error on executing function block", e);
      }
    }

    throw new OProcessException("Function '" + function + "' was not found");
  }

  @Override
  public String getName() {
    return NAME;
  }
}

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.parser.OVariableParser;
import com.orientechnologies.common.parser.OVariableParserListener;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.processor.OComposableProcessor;
import com.orientechnologies.orient.core.processor.OProcessException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLPredicate;

public abstract class OAbstractBlock implements OProcessorBlock {
  protected String returnVariable;

  protected abstract Object processBlock(OComposableProcessor iManager, OCommandContext iContext, ODocument iConfig,
      ODocument iOutput, boolean iReadOnly);

  @Override
  public Object process(OComposableProcessor iManager, OCommandContext iContext, ODocument iConfig, ODocument iOutput,
      boolean iReadOnly) {
    if (!checkForCondition(iContext, iConfig))
      return null;

    returnVariable = getFieldOfClass(iContext, iConfig, "return", String.class);

    debug(iContext, "Executing %s...", iConfig.field("type"));

    final Object result = processBlock(iManager, iContext, iConfig, iOutput, iReadOnly);

    debug(iContext, "Returned %s...", result);

    if (returnVariable != null)
      iContext.setVariable(returnVariable, result);

    return result;
  }

  protected Object delegate(final String iElementName, final OComposableProcessor iManager, final Object iContent,
      final OCommandContext iContext, ODocument iOutput, final boolean iReadOnly) {
    try {
      return iManager.process(iContent, iContext, iOutput, iReadOnly);
    } catch (Exception e) {
      throw new OProcessException("Error on processing '" + iElementName + "' field of '" + getName() + "' block", e);
    }
  }

  protected Object delegate(final String iElementName, final OComposableProcessor iManager, final String iType,
      final ODocument iContent, final OCommandContext iContext, ODocument iOutput, final boolean iReadOnly) {
    try {
      return iManager.process(iType, iContent, iContext, iOutput, iReadOnly);
    } catch (Exception e) {
      throw new OProcessException("Error on processing '" + iElementName + "' field of '" + getName() + "' block", e);
    }
  }

  public boolean checkForCondition(final OCommandContext iContext, final ODocument iConfig) {
    final String condition = getFieldOfClass(iContext, iConfig, "if", String.class);
    if (condition != null) {
      Object result = evaluate(iContext, condition);
      return result != null && (Boolean) result;
    }
    return true;
  }

  public Object evaluate(final OCommandContext iContext, final String iExpression) {
    if (iExpression == null)
      throw new OProcessException("Null expression");

    final OSQLPredicate predicate = new OSQLPredicate((String) resolveValue(iContext, iExpression));
    final Object result = predicate.evaluate(iContext);

    debug(iContext, "Evaluated expression '" + iExpression + "' = " + result);

    return result;
  }

  public void assignVariable(final OCommandContext iContext, final String iName, final Object iValue) {
    if (iName != null) {
      iContext.setVariable(iName, iValue);
      debug(iContext, "Assigned context variable %s=%s", iName, iValue);
    }
  }

  protected void debug(final OCommandContext iContext, final String iText, Object... iArgs) {
    if (isDebug(iContext)) {
      final Integer depthLevel = (Integer) iContext.getVariable("depthLevel");
      final StringBuilder text = new StringBuilder();
      for (int i = 0; i < depthLevel; ++i)
        text.append('-');
      text.append('>');
      text.append('{');
      text.append(getName());
      text.append("} ");
      text.append(iText);
      OLogManager.instance().info(this, text.toString(), iArgs);
    }
  }

  @SuppressWarnings("unchecked")
  protected <T> T getRawField(final ODocument iConfig, final String iFieldName) {
    return (T) iConfig.field(iFieldName);
  }

  @SuppressWarnings("unchecked")
  protected <T> T getField(final OCommandContext iContext, final ODocument iConfig, final String iFieldName) {
    return (T) resolveValue(iContext, iConfig.field(iFieldName));
  }

  @SuppressWarnings("unchecked")
  protected <T> T getFieldOrDefault(final OCommandContext iContext, final ODocument iConfig, final String iFieldName,
      final T iDefaultValue) {
    final Object f = iConfig.field(iFieldName);
    if (f == null)
      return iDefaultValue;
    return (T) resolveValue(iContext, f);
  }

  @SuppressWarnings("unchecked")
  protected <T> T getFieldOfClass(final OCommandContext iContext, final ODocument iConfig, final String iFieldName,
      Class<? extends T> iExpectedClass) {
    final Object f = resolveValue(iContext, iConfig.field(iFieldName));
    if (f != null)
      if (!iExpectedClass.isAssignableFrom(f.getClass()))
        throw new OProcessException("Block '" + getName() + "' defines the field '" + iFieldName + "' of type '" + f.getClass()
            + "' that is not compatible with the expected type '" + iExpectedClass + "'");

    return (T) f;
  }

  @SuppressWarnings("unchecked")
  protected <T> T getRequiredField(final OCommandContext iContext, final ODocument iConfig, final String iFieldName) {
    final Object f = iConfig.field(iFieldName);
    if (f == null)
      throw new OProcessException("Block '" + getName() + "' must define the field '" + iFieldName + "'");
    return (T) resolveValue(iContext, f);
  }

  @SuppressWarnings("unchecked")
  protected <T> T getRequiredFieldOfClass(final OCommandContext iContext, final ODocument iConfig, final String iFieldName,
      Class<? extends T> iExpectedClass) {
    final Object f = getFieldOfClass(iContext, iConfig, iFieldName, iExpectedClass);
    if (f == null)
      throw new OProcessException("Block '" + getName() + "' must define the field '" + iFieldName + "'");
    return (T) resolveValue(iContext, f);
  }

  public void checkForBlock(final Object iValue) {
    if (!isBlock(iValue))
      throw new OProcessException("Block '" + getName() + "' was expecting a block but found object of type " + iValue.getClass());
  }

  public boolean isDebug(final OCommandContext iContext) {
    final Object debug = iContext.getVariable("debugMode");
    if (debug != null) {
      if (debug instanceof Boolean)
        return (Boolean) debug;
      else
        return Boolean.parseBoolean((String) debug);
    }
    return false;
  }

  public static boolean isBlock(final Object iValue) {
    return iValue instanceof ODocument && ((ODocument) iValue).containsField(("type"));
  }

  public static Object resolve(final OCommandContext iContext, final Object iContent) {
    Object value = null;
    if (iContent instanceof String)
      value = OVariableParser.resolveVariables((String) iContent, OSystemVariableResolver.VAR_BEGIN,
          OSystemVariableResolver.VAR_END, new OVariableParserListener() {

            @Override
            public Object resolve(final String iVariable) {
              return iContext.getVariable(iVariable);
            }

          });
    else
      value = iContent;

    if (value instanceof String)
      value = OVariableParser.resolveVariables((String) value, "={", "}", new OVariableParserListener() {

        @Override
        public Object resolve(final String iVariable) {
          return new OSQLPredicate(iVariable).evaluate(iContext);
        }

      });

    return value;
  }

  @SuppressWarnings("unchecked")
  protected Object resolveValue(final OCommandContext iContext, final Object iValue) {
    if (iValue == null)
      return null;

    if (iValue instanceof String)
      // STRING
      return resolve(iContext, iValue);
    else if (iValue instanceof ODocument) {
      // DOCUMENT
      final ODocument sourceDoc = ((ODocument) iValue);
      final ODocument destDoc = new ODocument().setOrdered(true);
      for (String fieldName : sourceDoc.fieldNames()) {
        Object fieldValue = resolveValue(iContext, sourceDoc.field(fieldName));
        // PUT IN CONTEXT
        destDoc.field(fieldName, fieldValue);
      }
      return destDoc;

    } else if (iValue instanceof Map<?, ?>) {
      // MAP
      final Map<Object, Object> sourceMap = (Map<Object, Object>) iValue;
      final Map<Object, Object> destMap = new HashMap<Object, Object>();
      for (Entry<Object, Object> entry : sourceMap.entrySet())
        destMap.put(entry.getKey(), resolveValue(iContext, entry.getValue()));
      return destMap;

    } else if (iValue instanceof List<?>) {

      final List<Object> sourceList = (List<Object>) iValue;
      final List<Object> destList = new ArrayList<Object>();
      for (int i = 0; i < sourceList.size(); ++i)
        destList.add(i, resolve(iContext, sourceList.get(i)));
      return destList;
    }

    // ANY OTHER OBJECT
    return iValue;
  }
}
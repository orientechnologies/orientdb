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

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.parser.OVariableParser;
import com.orientechnologies.common.parser.OVariableParserListener;
import com.orientechnologies.common.util.OCollections;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.processor.OComposableProcessor;
import com.orientechnologies.orient.core.processor.OProcessException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLPredicate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public abstract class OAbstractBlock implements OProcessorBlock {
  protected OProcessorBlock parentBlock;

  protected abstract Object processBlock(OComposableProcessor iManager, OCommandContext iContext, ODocument iConfig,
      ODocument iOutput, boolean iReadOnly);

  @Override
  public Object process(OComposableProcessor iManager, OCommandContext iContext, ODocument iConfig, ODocument iOutput,
      boolean iReadOnly) {
    if (!checkForCondition(iContext, iConfig)) {
        return null;
    }

    Boolean enabled = getFieldOfClass(iContext, iConfig, "enabled", Boolean.class);
    if (enabled != null && !enabled) {
        return null;
    }

    String returnVariable = getFieldOfClass(iContext, iConfig, "return", String.class);

    debug(iContext, "Executing {%s} block...", iConfig.field("type"));

    final Object result = processBlock(iManager, iContext, iConfig, iOutput, iReadOnly);

    printReturn(iContext, result);

    if (returnVariable != null) {
        assignVariable(iContext, returnVariable, result);
    }

    return result;
  }

  protected void printReturn(OCommandContext iContext, final Object result) {
    if (result != null && !(result instanceof OCommandRequest) && result instanceof Iterable<?>) {
        debug(iContext, "Returned %s", OCollections.toString((Iterable<?>) result));
    } else {
        debug(iContext, "Returned %s", result);
    }
  }

  protected Object delegate(final String iElementName, final OComposableProcessor iManager, final Object iContent,
      final OCommandContext iContext, ODocument iOutput, final boolean iReadOnly) {
    try {
      return iManager.process(this, iContent, iContext, iOutput, iReadOnly);
    } catch (Exception e) {
      throw new OProcessException("Error on processing '" + iElementName + "' field of '" + getName() + "' block", e);
    }
  }

  protected Object delegate(final String iElementName, final OComposableProcessor iManager, final String iType,
      final ODocument iContent, final OCommandContext iContext, ODocument iOutput, final boolean iReadOnly) {
    try {
      return iManager.process(this, iType, iContent, iContext, iOutput, iReadOnly);
    } catch (Exception e) {
      throw new OProcessException("Error on processing '" + iElementName + "' field of '" + getName() + "' block", e);
    }
  }

  public boolean checkForCondition(final OCommandContext iContext, final ODocument iConfig) {
    Object condition = getField(iContext, iConfig, "if");
    if (condition instanceof Boolean) {
        return (Boolean) condition;
    } else if (condition != null) {
      Object result = evaluate(iContext, (String) condition);
      return result != null && (Boolean) result;
    }
    return true;
  }

  public Object evaluate(final OCommandContext iContext, final String iExpression) {
    if (iExpression == null) {
        throw new OProcessException("Null expression");
    }

    final OSQLPredicate predicate = new OSQLPredicate((String) resolveValue(iContext, iExpression, true));
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
      final StringBuilder text = new StringBuilder(256);
      for (int i = 0; i < depthLevel; ++i) {
          text.append('-');
      }
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
    return (T) resolveValue(iContext, iConfig.field(iFieldName), true);
  }

  @SuppressWarnings("unchecked")
  protected <T> T getField(final OCommandContext iContext, final ODocument iConfig, final String iFieldName, final boolean iCopy) {
    return (T) resolveValue(iContext, iConfig.field(iFieldName), iCopy);
  }

  @SuppressWarnings("unchecked")
  protected <T> T getFieldOrDefault(final OCommandContext iContext, final ODocument iConfig, final String iFieldName,
      final T iDefaultValue) {
    final Object f = iConfig.field(iFieldName);
    if (f == null) {
        return iDefaultValue;
    }
    return (T) resolveValue(iContext, f, true);
  }

  protected <T> T getFieldOfClass(final OCommandContext iContext, final ODocument iConfig, final String iFieldName,
      Class<? extends T> iExpectedClass) {
    return getFieldOfClass(iContext, iConfig, iFieldName, iExpectedClass, true);
  }

  @SuppressWarnings("unchecked")
  protected <T> T getFieldOfClass(final OCommandContext iContext, final ODocument iConfig, final String iFieldName,
      Class<? extends T> iExpectedClass, final boolean iCopy) {
    final Object f = resolveValue(iContext, iConfig.field(iFieldName), iCopy);
    if (f != null) {
        if (!iExpectedClass.isAssignableFrom(f.getClass())) {
            throw new OProcessException("Block '" + getName() + "' defines the field '" + iFieldName + "' of type '" + f.getClass()
                    + "' that is not compatible with the expected type '" + iExpectedClass + "'");
        }
    }

    return (T) f;
  }

  @SuppressWarnings("unchecked")
  protected <T> T getRequiredField(final OCommandContext iContext, final ODocument iConfig, final String iFieldName) {
    final Object f = iConfig.field(iFieldName);
    if (f == null) {
        throw new OProcessException("Block '" + getName() + "' must define the field '" + iFieldName + "'");
    }
    return (T) resolveValue(iContext, f, true);
  }

  @SuppressWarnings("unchecked")
  protected <T> T getRequiredFieldOfClass(final OCommandContext iContext, final ODocument iConfig, final String iFieldName,
      Class<? extends T> iExpectedClass) {
    final Object f = getFieldOfClass(iContext, iConfig, iFieldName, iExpectedClass);
    if (f == null) {
        throw new OProcessException("Block '" + getName() + "' must define the field '" + iFieldName + "'");
    }
    return (T) resolveValue(iContext, f, true);
  }

  public void checkForBlock(final Object iValue) {
    if (!isBlock(iValue)) {
        throw new OProcessException("Block '" + getName() + "' was expecting a block but found object of type " + iValue.getClass());
    }
  }

  public boolean isDebug(final OCommandContext iContext) {
    final Object debug = iContext.getVariable("debugMode");
    if (debug != null) {
      if (debug instanceof Boolean) {
          return (Boolean) debug;
      } else {
          return Boolean.parseBoolean((String) debug);
      }
    }
    return false;
  }

  public static boolean isBlock(final Object iValue) {
    return iValue instanceof ODocument && ((ODocument) iValue).containsField(("type"));
  }

  public static Object resolve(final OCommandContext iContext, final Object iContent) {
    Object value = null;
    if (iContent instanceof String) {
        value = OVariableParser.resolveVariables((String) iContent, OSystemVariableResolver.VAR_BEGIN,
                OSystemVariableResolver.VAR_END, new OVariableParserListener() {
                    
                    @Override
                    public Object resolve(final String iVariable) {
                        return iContext.getVariable(iVariable);
                    }
                    
                });
    } else {
        value = iContent;
    }

    if (value instanceof String) {
        value = OVariableParser.resolveVariables((String) value, "={", "}", new OVariableParserListener() {
            
            @Override
            public Object resolve(final String iVariable) {
                return new OSQLPredicate(iVariable).evaluate(iContext);
            }
            
        });
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  protected Object resolveValue(final OCommandContext iContext, final Object iValue, final boolean iClone) {
    if (iValue == null) {
        return null;
    }

    if (iValue instanceof String) {
        // STRING
        return resolve(iContext, iValue);
    } else if (iValue instanceof ODocument) {
      // DOCUMENT
      final ODocument sourceDoc = ((ODocument) iValue);
      final ODocument destDoc = iClone ? new ODocument().setOrdered(true) : sourceDoc;
      for (String fieldName : sourceDoc.fieldNames()) {
        Object fieldValue = resolveValue(iContext, sourceDoc.field(fieldName), iClone);
        // PUT IN CONTEXT
        destDoc.field(fieldName, fieldValue);
      }
      return destDoc;

    } else if (iValue instanceof Map<?, ?>) {
      // MAP
      final Map<Object, Object> sourceMap = (Map<Object, Object>) iValue;
      final Map<Object, Object> destMap = iClone ? new HashMap<Object, Object>() : sourceMap;
      for (Entry<Object, Object> entry : sourceMap.entrySet()) {
          destMap.put(entry.getKey(), resolveValue(iContext, entry.getValue(), iClone));
      }
      return destMap;

    } else if (iValue instanceof List<?>) {

      final List<Object> sourceList = (List<Object>) iValue;
      final List<Object> destList = iClone ? new ArrayList<Object>() : sourceList;
      for (int i = 0; i < sourceList.size(); ++i) {
          if (iClone) {
              destList.add(i, resolve(iContext, sourceList.get(i)));
          } else {
              destList.set(i, resolve(iContext, sourceList.get(i)));
          }
      }

      return destList;
    }

    // ANY OTHER OBJECT
    return iValue;
  }

  @SuppressWarnings("unchecked")
  protected Object getValue(final Object iValue, final Boolean iCopy) {
    if (iValue != null && iCopy != null && iCopy) {
      // COPY THE VALUE
      if (iValue instanceof ODocument) {
          return ((ODocument) iValue).copy();
      } else if (iValue instanceof List) {
          return new ArrayList<Object>((Collection<Object>) iValue);
      } else if (iValue instanceof Set) {
          return new HashSet<Object>((Collection<Object>) iValue);
      } else if (iValue instanceof Map) {
          return new LinkedHashMap<Object, Object>((Map<Object, Object>) iValue);
      } else {
          throw new OProcessException("Copy of value '" + iValue + "' of class '" + iValue.getClass() + "' is not supported");
      }
    }
    return iValue;
  }

  protected Object flatMultivalues(final OCommandContext iContext, final Boolean copy, final Boolean flatMultivalues,
      final Object value) {
    if (OMultiValue.isMultiValue(value) && flatMultivalues != null && flatMultivalues) {
      Collection<Object> newColl;
      if (value instanceof Set<?>) {
          newColl = new HashSet<Object>();
      } else {
          newColl = new ArrayList<Object>();
      }

      for (Object entry : OMultiValue.getMultiValueIterable(value)) {
        if (entry instanceof ODocument) {
          // DOCUMENT
          for (String fieldName : ((ODocument) entry).fieldNames()) {
              newColl.add(((ODocument) entry).field(fieldName));
          }
        } else {
            OMultiValue.add(newColl, resolveValue(iContext, getValue(entry, copy), false));
        }
      }

      return newColl;
    }
    return value;
  }

  public OProcessorBlock getParentBlock() {
    return parentBlock;
  }

  public void setParentBlock(OProcessorBlock parentBlock) {
    this.parentBlock = parentBlock;
  }

}

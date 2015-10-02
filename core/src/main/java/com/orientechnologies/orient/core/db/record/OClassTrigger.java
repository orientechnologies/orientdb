/*
 * Copyright 2010-2012 henryzhao81-at-gmail.com
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

package com.orientechnologies.orient.core.db.record;

import java.lang.reflect.Method;

import javax.script.Bindings;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import com.orientechnologies.common.concur.resource.OPartitionedObjectPool;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.script.OCommandScriptException;
import com.orientechnologies.orient.core.command.script.OScriptManager;
import com.orientechnologies.orient.core.db.ODatabase.STATUS;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.hook.ODocumentHookAbstract;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;

/**
 * Author : henryzhao81@gmail.com Feb 19, 2013
 * 
 * Create a class OTriggered which contains 8 additional class attributes, which link to OFunction - beforeCreate - afterCreate -
 * beforeRead - afterRead - beforeUpdate - afterUpdate - beforeDelete - afterDelete
 */
public class OClassTrigger extends ODocumentHookAbstract {
  public static final String CLASSNAME          = "OTriggered";
  public static final String METHOD_SEPARATOR   = ".";

  // Class Level Trigger (class custom attribute)
  public static final String ONBEFORE_CREATED   = "onBeforeCreate";
  // Record Level Trigger (property name)
  public static final String PROP_BEFORE_CREATE = ONBEFORE_CREATED;
  public static final String ONAFTER_CREATED    = "onAfterCreate";
  public static final String PROP_AFTER_CREATE  = ONAFTER_CREATED;
  public static final String ONBEFORE_READ      = "onBeforeRead";
  public static final String PROP_BEFORE_READ   = ONBEFORE_READ;
  public static final String ONAFTER_READ       = "onAfterRead";
  public static final String PROP_AFTER_READ    = ONAFTER_READ;
  public static final String ONBEFORE_UPDATED   = "onBeforeUpdate";
  public static final String PROP_BEFORE_UPDATE = ONBEFORE_UPDATED;
  public static final String ONAFTER_UPDATED    = "onAfterUpdate";
  public static final String PROP_AFTER_UPDATE  = ONAFTER_UPDATED;
  public static final String ONBEFORE_DELETE    = "onBeforeDelete";
  public static final String PROP_BEFORE_DELETE = ONBEFORE_DELETE;
  public static final String ONAFTER_DELETE     = "onAfterDelete";
  public static final String PROP_AFTER_DELETE  = ONAFTER_DELETE;

  public OClassTrigger(ODatabaseDocument database) {
    super(database);
  }

  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE;
  }

  @Override
  public RESULT onRecordBeforeCreate(final ODocument iDocument) {
    Object func = this.checkClzAttribute(iDocument, ONBEFORE_CREATED);
    if (func != null) {
      if (func instanceof OFunction)
        return this.executeFunction(iDocument, (OFunction) func);
      else if (func instanceof Object[])
        return this.executeMethod(iDocument, (Object[]) func);
    }
    return RESULT.RECORD_NOT_CHANGED;
  }

  @Override
  public void onRecordAfterCreate(final ODocument iDocument) {
    Object func = this.checkClzAttribute(iDocument, ONAFTER_CREATED);
    if (func != null) {
      if (func instanceof OFunction)
        this.executeFunction(iDocument, (OFunction) func);
      else if (func instanceof Object[])
        this.executeMethod(iDocument, (Object[]) func);
    }
  }

  @Override
  public RESULT onRecordBeforeRead(final ODocument iDocument) {
    Object func = this.checkClzAttribute(iDocument, ONBEFORE_READ);
    if (func != null) {
      if (func instanceof OFunction)
        return this.executeFunction(iDocument, (OFunction) func);
      else if (func instanceof Object[])
        return this.executeMethod(iDocument, (Object[]) func);
    }
    return RESULT.RECORD_NOT_CHANGED;
  }

  @Override
  public void onRecordAfterRead(final ODocument iDocument) {
    Object func = this.checkClzAttribute(iDocument, ONAFTER_READ);
    if (func != null) {
      if (func instanceof OFunction)
        this.executeFunction(iDocument, (OFunction) func);
      else if (func instanceof Object[])
        this.executeMethod(iDocument, (Object[]) func);
    }
  }

  @Override
  public RESULT onRecordBeforeUpdate(final ODocument iDocument) {
    Object func = this.checkClzAttribute(iDocument, ONBEFORE_UPDATED);
    if (func != null) {
      if (func instanceof OFunction)
        return this.executeFunction(iDocument, (OFunction) func);
      else if (func instanceof Object[])
        return this.executeMethod(iDocument, (Object[]) func);
    }
    return RESULT.RECORD_NOT_CHANGED;
  }

  @Override
  public void onRecordAfterUpdate(final ODocument iDocument) {
    Object func = this.checkClzAttribute(iDocument, ONAFTER_UPDATED);
    if (func != null) {
      if (func instanceof OFunction)
        this.executeFunction(iDocument, (OFunction) func);
      else if (func instanceof Object[])
        this.executeMethod(iDocument, (Object[]) func);
    }
  }

  @Override
  public RESULT onRecordBeforeDelete(final ODocument iDocument) {
    Object func = this.checkClzAttribute(iDocument, ONBEFORE_DELETE);
    if (func != null) {
      if (func instanceof OFunction)
        return this.executeFunction(iDocument, (OFunction) func);
      else if (func instanceof Object[])
        return this.executeMethod(iDocument, (Object[]) func);
    }
    return RESULT.RECORD_NOT_CHANGED;
  }

  @Override
  public void onRecordAfterDelete(final ODocument iDocument) {
    Object func = this.checkClzAttribute(iDocument, ONAFTER_DELETE);
    if (func != null) {
      if (func instanceof OFunction)
        this.executeFunction(iDocument, (OFunction) func);
      else if (func instanceof Object[])
        this.executeMethod(iDocument, (Object[]) func);
    }
  }

  public RESULT onTrigger(final TYPE iType, final ORecord iRecord) {
    if (database.getStatus() != STATUS.OPEN)
      return RESULT.RECORD_NOT_CHANGED;

    if (!(iRecord instanceof ODocument))
      return RESULT.RECORD_NOT_CHANGED;

    final ODocument document = (ODocument) iRecord;
    OImmutableClass immutableSchemaClass = ODocumentInternal.getImmutableSchemaClass(document);
    if (immutableSchemaClass != null && immutableSchemaClass.isTriggered())
      return super.onTrigger(iType, iRecord);

    return RESULT.RECORD_NOT_CHANGED;
  }

  private Object checkClzAttribute(final ODocument iDocument, String attr) {
    final OImmutableClass clz = ODocumentInternal.getImmutableSchemaClass(iDocument);
    if (clz != null && clz.isTriggered()) {
      OFunction func = null;
      String fieldName = clz.getCustom(attr);
      OClass superClz = clz.getSuperClass();
      while (fieldName == null || fieldName.length() == 0) {
        if (superClz == null || superClz.getName().equals(CLASSNAME))
          break;
        fieldName = superClz.getCustom(attr);
        superClz = superClz.getSuperClass();
      }
      if (fieldName != null && fieldName.length() > 0) {
        // check if it is reflection or not
        final Object[] clzMethod = this.checkMethod(fieldName);
        if (clzMethod != null)
          return clzMethod;
        func = database.getMetadata().getFunctionLibrary().getFunction(fieldName);
        if (func == null) { // check if it is rid
          if (OStringSerializerHelper.contains(fieldName, ORID.SEPARATOR)) {
            try {
              ODocument funcDoc = database.load(new ORecordId(fieldName));
              if (funcDoc != null) {
                func = database.getMetadata().getFunctionLibrary().getFunction((String) funcDoc.field("name"));
              }
            } catch (Exception ex) {
              OLogManager.instance().error(this, "illegal record id : ", ex.getMessage());
            }
          }
        }
      } else {
        final Object funcProp = iDocument.field(attr);
        if (funcProp != null) {
          final String funcName = funcProp instanceof ODocument ? (String) ((ODocument) funcProp).field("name") : funcProp
              .toString();
          func = database.getMetadata().getFunctionLibrary().getFunction(funcName);
        }
      }
      return func;
    }
    return null;
  }

  private Object[] checkMethod(String fieldName) {
    String clzName = null;
    String methodName = null;
    if (fieldName.contains(METHOD_SEPARATOR)) {
      clzName = fieldName.substring(0, fieldName.lastIndexOf(METHOD_SEPARATOR));
      methodName = fieldName.substring(fieldName.lastIndexOf(METHOD_SEPARATOR) + 1);
    }
    if (clzName == null || methodName == null)
      return null;
    try {
      Class clz = ClassLoader.getSystemClassLoader().loadClass(clzName);
      Method method = clz.getMethod(methodName, ODocument.class);
      return new Object[] { clz, method };
    } catch (Exception ex) {
      OLogManager.instance().error(this, "illegal class or method : " + clzName + "/" + methodName);
      return null;
    }
  }

  private RESULT executeMethod(final ODocument iDocument, final Object[] clzMethod) {
    if (clzMethod[0] instanceof Class && clzMethod[1] instanceof Method) {
      Method method = (Method) clzMethod[1];
      Class clz = (Class) clzMethod[0];
      String result = null;
      try {
        result = (String) method.invoke(clz.newInstance(), iDocument);
      } catch (Exception ex) {
        throw OException.wrapException(new ODatabaseException("Failed to invoke method " + method.getName()), ex);
      }
      if (result == null) {
        return RESULT.RECORD_NOT_CHANGED;
      }
      return RESULT.valueOf(result);
    }
    return RESULT.RECORD_NOT_CHANGED;
  }

  private RESULT executeFunction(final ODocument iDocument, final OFunction func) {
    if (func == null)
      return RESULT.RECORD_NOT_CHANGED;

    final OScriptManager scriptManager = Orient.instance().getScriptManager();

    final OPartitionedObjectPool.PoolEntry<ScriptEngine> entry = scriptManager.acquireDatabaseEngine(database.getName(),
        func.getLanguage());
    final ScriptEngine scriptEngine = entry.object;
    try {
      final Bindings binding = scriptEngine.getBindings(ScriptContext.ENGINE_SCOPE);

      scriptManager.bind(binding, (ODatabaseDocumentTx) database, null, null);
      binding.put("doc", iDocument);

      String result = null;
      try {
        if (func.getLanguage() == null)
          throw new OConfigurationException("Database function '" + func.getName() + "' has no language");
        final String funcStr = scriptManager.getFunctionDefinition(func);
        if (funcStr != null) {
          try {
            scriptEngine.eval(funcStr);
          } catch (ScriptException e) {
            scriptManager.throwErrorMessage(e, funcStr);
          }
        }
        if (scriptEngine instanceof Invocable) {
          final Invocable invocableEngine = (Invocable) scriptEngine;
          Object[] EMPTY = OCommonConst.EMPTY_OBJECT_ARRAY;
          result = (String) invocableEngine.invokeFunction(func.getName(), EMPTY);
        }
      } catch (ScriptException e) {
        throw OException.wrapException(
            new OCommandScriptException("Error on execution of the script", func.getName(), e.getColumnNumber()), e);
      } catch (NoSuchMethodException e) {
        throw OException.wrapException(new OCommandScriptException("Error on execution of the script", func.getName(), 0), e);
      } catch (OCommandScriptException e) {
        // PASS THROUGH
        throw e;

      } finally {
        scriptManager.unbind(binding, null, null);
      }
      if (result == null) {
        return RESULT.RECORD_NOT_CHANGED;
      }
      return RESULT.valueOf(result);

    } finally {
      scriptManager.releaseDatabaseEngine(func.getLanguage(), database.getName(), entry);
    }
  }
}

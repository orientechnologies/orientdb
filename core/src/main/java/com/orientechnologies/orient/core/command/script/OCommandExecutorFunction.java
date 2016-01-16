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
package com.orientechnologies.orient.core.command.script;

import com.orientechnologies.common.concur.resource.OPartitionedObjectPool;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandExecutorAbstract;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;

import javax.script.*;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Executes Script Commands.
 * 
 * @see OCommandScript
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorFunction extends OCommandExecutorAbstract {
  protected OCommandFunction request;

  public OCommandExecutorFunction() {
  }

  @SuppressWarnings("unchecked")
  public OCommandExecutorFunction parse(final OCommandRequest iRequest) {
    request = (OCommandFunction) iRequest;
    return this;
  }

  public Object execute(final Map<Object, Object> iArgs) {
    return executeInContext(null, iArgs);
  }

  public Object executeInContext(final OCommandContext iContext, final Map<Object, Object> iArgs) {

    parserText = request.getText();

    ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.INSTANCE.get();
    final OFunction f = db.getMetadata().getFunctionLibrary().getFunction(parserText);

    db.checkSecurity(ORule.ResourceGeneric.FUNCTION, ORole.PERMISSION_READ, f.getName());

    final OScriptManager scriptManager = Orient.instance().getScriptManager();

    final OPartitionedObjectPool.PoolEntry<ScriptEngine> entry = scriptManager.acquireDatabaseEngine(db.getName(), f.getLanguage());
    final ScriptEngine scriptEngine = entry.object;
    try {
      final Bindings binding = scriptManager.bind(scriptEngine.getBindings(ScriptContext.ENGINE_SCOPE), (ODatabaseDocumentTx) db,
          iContext, iArgs);

      try {
        final Object result;

        if (scriptEngine instanceof Invocable) {
          // INVOKE AS FUNCTION. PARAMS ARE PASSED BY POSITION
          final Invocable invocableEngine = (Invocable) scriptEngine;
          Object[] args = null;
          if (iArgs != null) {
            args = new Object[iArgs.size()];
            int i = 0;
            for (Entry<Object, Object> arg : iArgs.entrySet())
              args[i++] = arg.getValue();
          } else {
        	  args = OCommonConst.EMPTY_OBJECT_ARRAY;
          }
          result = invocableEngine.invokeFunction(parserText, args);

        } else {
          // INVOKE THE CODE SNIPPET
          final Object[] args = iArgs == null ? null : iArgs.values().toArray();
          result = scriptEngine.eval(scriptManager.getFunctionInvoke(f, args), binding);
        }

        return OCommandExecutorUtility.transformResult(result);

      } catch (ScriptException e) {
        throw new OCommandScriptException("Error on execution of the script", request.getText(), e.getColumnNumber(), e);
      } catch (NoSuchMethodException e) {
        throw new OCommandScriptException("Error on execution of the script", request.getText(), 0, e);
      } catch (OCommandScriptException e) {
        // PASS THROUGH
        throw e;

      } finally {
        scriptManager.unbind(binding, iContext, iArgs);
      }
    } finally {
      scriptManager.releaseDatabaseEngine(f.getLanguage(), db.getName(), entry);
    }
  }

  public boolean isIdempotent() {
    return false;
  }

  @Override
  protected void throwSyntaxErrorException(String iText) {
    throw new OCommandScriptException("Error on execution of the script: " + iText, request.getText(), 0);
  }
}

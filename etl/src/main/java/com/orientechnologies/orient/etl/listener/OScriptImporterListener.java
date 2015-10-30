/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.etl.listener;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.HashMap;
import java.util.Map;

public class OScriptImporterListener implements OImporterListener {
  private final Map<String, String>   events;
  private Map<String, OCommandScript> scripts = new HashMap<String, OCommandScript>();

  public OScriptImporterListener() {
    events = new HashMap<String, String>();
  }

  public OScriptImporterListener(final Map<String, String> iEvents) {
    events = iEvents;
  }

  @Override
  public void onBeforeFile(final ODatabaseDocumentTx db, final OCommandContext iContext) {
    executeEvent(db, "onBeforeFile", iContext);
  }

  @Override
  public void onAfterFile(final ODatabaseDocumentTx db, final OCommandContext iContext) {
    executeEvent(db, "onAfterFile", iContext);
  }

  @Override
  public boolean onBeforeLine(final ODatabaseDocumentTx db, final OCommandContext iContext) {
    final Object ret = executeEvent(db, "onBeforeLine", iContext);
    if (ret != null && ret instanceof Boolean)
      return (Boolean) ret;
    return true;
  }

  @Override
  public void onAfterLine(final ODatabaseDocumentTx db, final OCommandContext iContext) {
    executeEvent(db, "onAfterLine", iContext);
  }

  @Override
  public void onDump(final ODatabaseDocumentTx db, final OCommandContext iContext) {
    executeEvent(db, "onDump", iContext);
  }

  @Override
  public void onJoinNotFound(ODatabaseDocumentTx db, OCommandContext iContext, OIndex<?> iIndex, Object iKey) {
    executeEvent(db, "onJoinNotFound", iContext);
  }

  @Override
  public void validate(ODatabaseDocumentTx db, OCommandContext iContext, ODocument iRecord) {
  }

  private Object executeEvent(final ODatabaseDocumentTx db, final String iEventName, final OCommandContext iContext) {
    if (events == null)
      return null;

    OCommandScript script = scripts.get(iEventName);

    if (script == null) {
      final String code = events.get(iEventName);
      if (code != null) {
        // CACHE IT
        script = new OCommandScript(code).setLanguage("Javascript");
        scripts.put(iEventName, script);
      }
    }

    if (script != null) {
      final Map<String, Object> pars = new HashMap<String, Object>();
      pars.put("task", iContext);
      pars.put("importer", this);

      return db.command(script).execute(pars);
    }
    return null;
  }

}

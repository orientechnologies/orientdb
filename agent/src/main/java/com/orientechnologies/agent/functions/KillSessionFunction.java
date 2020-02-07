package com.orientechnologies.agent.functions;

import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;

/**
 * Created by Enrico Risa on 23/07/2018.
 */
public class KillSessionFunction extends OSQLEnterpriseFunction {

  private OEnterpriseServer server;

  public KillSessionFunction(OEnterpriseServer server) {
    super("killSession", 1, 1);

    this.server = server;
  }

  @Override
  public Object exec(Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult, Object[] iParams,
      OCommandContext iContext) {
    if (iParams[0] instanceof Number) {
      Number connectionId = (Number) iParams[0];
      ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
      server.interruptConnection(connectionId.intValue());
      if (db != null) {
        db.activateOnCurrentThread();
      }
      OResultInternal internal = new OResultInternal();
      internal.setProperty("message", String.format("Connection %s interrupted", connectionId));
      return internal;
    } else {
      throw new IllegalArgumentException("Connection id muse be a number");
    }
  }

  @Override
  public String getSyntax() {
    return "killSession(<connectionId>)";
  }

  @Override
  public ORule.ResourceGeneric genericPermission() {
    return ORule.ResourceGeneric.DATABASE;
  }

  @Override
  public String specificPermission() {
    return "killSession";
  }

}

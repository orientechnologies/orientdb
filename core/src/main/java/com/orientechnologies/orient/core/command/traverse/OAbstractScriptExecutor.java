package com.orientechnologies.orient.core.command.traverse;

import com.orientechnologies.orient.core.command.OScriptExecutor;
import com.orientechnologies.orient.core.command.OScriptInterceptor;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import java.util.ArrayList;
import java.util.List;

public abstract class OAbstractScriptExecutor implements OScriptExecutor {

  protected String language;

  public OAbstractScriptExecutor(String language) {
    this.language = language;
  }

  private List<OScriptInterceptor> interceptors = new ArrayList<>();

  @Override
  public void registerInterceptor(OScriptInterceptor interceptor) {
    interceptors.add(interceptor);
  }

  public void preExecute(ODatabaseDocumentInternal database, String script, Object params) {

    interceptors.forEach(i -> i.preExecute(database, language, script, params));
  }

  @Override
  public void unregisterInterceptor(OScriptInterceptor interceptor) {
    interceptors.remove(interceptor);
  }
}

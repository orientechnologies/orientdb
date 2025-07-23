package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.command.script.OScriptManager;
import com.orientechnologies.orient.core.db.OrientDBEmbedded;
import java.util.List;

/** Created by Enrico Risa on 25/01/17. */
public interface OScriptExecutorRegister {
  void registerExecutor(OrientDBEmbedded ctx, OScriptManager scriptManager);

  List<String> getLanguages();

  int getPriority();
}

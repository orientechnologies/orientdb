package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.command.script.OScriptManager;

/** Created by Enrico Risa on 25/01/17. */
public interface OScriptExecutorRegister {
  void registerExecutor(OScriptManager scriptManager, OCommandManager commandManager);
}

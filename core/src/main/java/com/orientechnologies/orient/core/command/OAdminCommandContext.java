package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.db.OrientDBEmbedded;

public interface OAdminCommandContext extends OCommandContext {

  OrientDBEmbedded getGlobalContext();
}

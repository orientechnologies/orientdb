package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.db.OrientDBInternal;

public interface OAdminCommandContext extends OCommandContext {

  OrientDBInternal getGlobalContext();
}

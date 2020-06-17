package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.record.impl.ODocument;

public interface SimpleValueFetchPlanCommandListener extends OCommandResultListener {

  void linkdedBySimpleValue(ODocument doc);
}

package com.orientechnologies.lucene;

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

/**
 * Created by enricorisa on 21/03/14.
 */
public class OEnterpriseLuceneIndex extends OServerPluginAbstract {

  public OEnterpriseLuceneIndex() {
  }

  @Override
  public String getName() {
    return "enterprise-lucene";
  }

  @Override
  public void startup() {
    super.startup();
    // Orient.instance().addDbLifecycleListener(new OLuceneClassIndexManager());
  }

  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {

  }
}

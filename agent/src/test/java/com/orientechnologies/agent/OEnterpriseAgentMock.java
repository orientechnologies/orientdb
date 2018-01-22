package com.orientechnologies.agent;

import com.orientechnologies.agent.profiler.OEnterpriseProfiler;
import com.orientechnologies.common.profiler.OAbstractProfiler;
import com.orientechnologies.orient.core.Orient;

/**
 * Created by Enrico Risa on 03/10/16.
 */
public class OEnterpriseAgentMock extends OEnterpriseAgent {

  @Override
  protected void installProfiler() {
    final OAbstractProfiler currentProfiler = (OAbstractProfiler) Orient.instance().getProfiler();

    profiler = new OEnterpriseProfiler(60, currentProfiler, server, this) {

      // do not shutdown in test
      @Override
      public void shutdown() {

      }
    };

    Orient.instance().setProfiler(profiler);
    Orient.instance().getProfiler().startup();

    currentProfiler.shutdown();
  }
}

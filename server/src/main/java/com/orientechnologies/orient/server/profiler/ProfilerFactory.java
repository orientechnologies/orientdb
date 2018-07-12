package com.orientechnologies.orient.server.profiler;

import com.orientechnologies.common.profiler.OrientDBProfiler;
import com.orientechnologies.orient.server.OServer;

/**
 * Created by Enrico Risa on 11/07/2018.
 */
public interface ProfilerFactory {

  OrientDBProfiler createProfilerFor(OServer server);
}

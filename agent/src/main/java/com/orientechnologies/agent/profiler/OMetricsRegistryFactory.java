package com.orientechnologies.agent.profiler;

import com.orientechnologies.agent.services.metrics.OrientDBMetricsSettings;
import com.orientechnologies.enterprise.server.OEnterpriseServer;

/**
 * Created by Enrico Risa on 11/07/2018.
 */
public class OMetricsRegistryFactory {

  public static OMetricsRegistry createRegistryFor(OEnterpriseServer server, OrientDBMetricsSettings settings) {

    return new ODropWizardMetricsRegistry(server, settings);
  }

}

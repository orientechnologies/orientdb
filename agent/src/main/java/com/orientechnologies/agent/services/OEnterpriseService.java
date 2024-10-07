package com.orientechnologies.agent.services;

import com.orientechnologies.enterprise.server.OEnterpriseServer;

/** Created by Enrico Risa on 13/07/2018. */
public interface OEnterpriseService {

  void init(OEnterpriseServer server);

  void start();

  void stop();
}

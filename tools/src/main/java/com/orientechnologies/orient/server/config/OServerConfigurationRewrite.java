package com.orientechnologies.orient.server.config;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.config.distributed.OServerDistributedConfiguration;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

public class OServerConfigurationRewrite {

  public void rewrite(String file) throws IOException {
    var loader = new OServerConfigurationLoaderXml(OServerConfiguration.class, new File(file));
    OServerConfiguration load = loader.load();
    migrateDistributed(load, false);
    loader.save(load);
  }

  private void migrateDistributed(OServerConfiguration load, boolean removeOld) throws IOException {
    Iterator<OServerHandlerConfiguration> iter = load.handlers.iterator();
    while (iter.hasNext()) {
      OServerHandlerConfiguration handler = iter.next();
      if (handler.clazz.equals(
          "com.orientechnologies.orient.server.distributed.impl.ODistributedPlugin")) {
        if (removeOld) {
          iter.remove();
        }
        load.distributed = new OServerDistributedConfiguration();

        for (OServerParameterConfiguration par : handler.parameters) {
          if ("enabled".equalsIgnoreCase(par.name)) {
            load.distributed.enabled = Boolean.valueOf(par.value);
          }
          if ("nodeName".equalsIgnoreCase(par.name)) {
            load.distributed.nodeName = par.value;
          }
          if ("configuration.db.default".equalsIgnoreCase(par.name)) {
            String config = Files.readString(Path.of(par.value));
            var c = new ODocument();
            c.fromJSON(config);
            String quorumValue = c.getProperty("writeQuorum");
            if ("majority".equals(quorumValue)) {
              load.distributed.quorum = 2;
            } else {
              try {
                load.distributed.quorum = Integer.valueOf(quorumValue);
              } catch (NumberFormatException e) {
              }
            }
            if (load.distributed.quorum == 0) {
              load.distributed.quorum = 2;
            }
          }
        }
      }
    }
  }
}

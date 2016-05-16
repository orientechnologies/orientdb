/*
 * Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */

package com.orientechnologies.agent.backup;

import com.orientechnologies.agent.backup.log.OBackupLogger;
import com.orientechnologies.agent.backup.strategy.OBackupStrategy;
import com.orientechnologies.agent.backup.strategy.OBackupStrategyFullBackup;
import com.orientechnologies.agent.backup.strategy.OBackupStrategyIncrementalBackup;
import com.orientechnologies.agent.backup.strategy.OBackupStrategyMixBackup;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.schedule.OCronExpression;
import com.orientechnologies.orient.server.handler.OAutomaticBackup;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;

/**
 * Created by Enrico Risa on 22/03/16.
 */
public class OBackupConfig {

  public static final String     BACKUPS    = "backups";
  private ODocument              configuration;
  private Map<String, ODocument> configs    = new HashMap<String, ODocument>();

  public static final String     DBNAME     = "dbName";
  public static final String     WHEN       = "when";
  public static final String     DIRECTORY  = "directory";
  public static final String     MODES      = "modes";
  public static final String     ID         = "uuid";

  private String                 configFile = "${ORIENTDB_HOME}/config/backups.json";

  public OBackupConfig() {
    this.configuration = new ODocument();
    configuration.field("backups", new ArrayList<ODocument>());
  }

  public OBackupConfig load() {

    final File f = new File(OSystemVariableResolver.resolveSystemVariables(configFile));

    if (f.exists()) {
      // READ THE FILE
      try {
        final String configurationContent = OIOUtils.readFileAsString(f);
        configuration = new ODocument().fromJSON(configurationContent, "noMap");
      } catch (IOException e) {
        OException.wrapException(new OConfigurationException(
            "Cannot load Backups configuration file '" + configFile + "'. Backups  Plugin will be disabled"), e);
      }
    } else {
      try {
        f.getParentFile().mkdirs();
        f.createNewFile();
        OIOUtils.writeFile(f, configuration.toJSON("prettyPrint"));

        OLogManager.instance().info(this, "Backups plugin: created configuration to file '%s'", f);
      } catch (IOException e) {
        OException.wrapException(new OConfigurationException(
            "Backups create Events plugin configuration file '" + configFile + "'. Backups Plugin will be disabled"), e);
      }
    }

    return this;
  }

  public Collection<ODocument> backups() {
    synchronized (this) {
      return configuration.field(BACKUPS);
    }
  }

  public ODocument getConfig() {
    synchronized (this) {
      return configuration;
    }
  }

  public ODocument addBackup(ODocument doc) {

    synchronized (this) {

      String uuid = UUID.randomUUID().toString();
      doc.field(ID, uuid);
      validateBackup(doc);
      pushBackup(doc);
      return doc;
    }
  }

  private void pushBackup(ODocument doc) {
    Collection<ODocument> backups = configuration.field(BACKUPS);
    backups.add(doc);
    writeConfiguration();
  }

  protected void validateSingleMode(ODocument doc) {
    if (!doc.containsField(WHEN)) {
      throw new OConfigurationException("Field when is required");
    } else {
      String when = doc.field(WHEN);
      try {
        new OCronExpression(when);
      } catch (ParseException e) {
        throw new OConfigurationException("When is not a valid Cron expression");
      }
    }
  }

  protected void validateModes(ODocument modes) {

    ODocument incremental = modes.field(OAutomaticBackup.MODE.INCREMENTAL_BACKUP.toString());
    ODocument full = modes.field(OAutomaticBackup.MODE.FULL_BACKUP.toString());

    if (incremental == null && full == null) {
      throw new OConfigurationException("Field mode is invalid: supported mode are FULL_BACKUP,INCREMENTAL_BACKUP");
    }

    if (incremental != null) {
      validateSingleMode(incremental);
    }
    if (full != null) {
      validateSingleMode(full);
    }
  }

  protected void validateBackup(ODocument doc) {

    if (!doc.containsField(DBNAME)) {
      throw new OConfigurationException("Field dbName is required");
    }
    if (!doc.containsField(DIRECTORY)) {
      throw new OConfigurationException("Field directory is required");
    }
    if (!doc.containsField(MODES)) {
      throw new OConfigurationException("Field modes is required");
    } else {
      ODocument modes = doc.field(MODES);
      validateModes(modes);

    }

  }

  public ODocument changeBackup(String uuid, ODocument doc) {
    validateBackup(doc);
    removeBackup(uuid, doc);
    pushBackup(doc);
    return doc;
  }

  public void removeBackup(String uuid, ODocument doc) {
    synchronized (this) {
      Collection<ODocument> backups = configuration.field(BACKUPS);
      ODocument toRemove = null;
      for (ODocument backup : backups) {
        if (backup.field(ID).equals(uuid)) {
          toRemove = backup;
        }
      }
      if (toRemove != null) {
        backups.remove(toRemove);
      }
      writeConfiguration();
    }
  }

  public OBackupStrategy strategy(ODocument cfg, OBackupLogger logger) {

    ODocument full = (ODocument) cfg.eval(OBackupConfig.MODES + "." + OAutomaticBackup.MODE.FULL_BACKUP);

    ODocument incremental = (ODocument) cfg.eval(OBackupConfig.MODES + "." + OAutomaticBackup.MODE.INCREMENTAL_BACKUP);

    OBackupStrategy strategy;
    if (full != null && incremental != null) {
      strategy = new OBackupStrategyMixBackup(cfg, logger);
    } else if (full != null) {
      strategy = new OBackupStrategyFullBackup(cfg, logger);
    } else {
      strategy = new OBackupStrategyIncrementalBackup(cfg, logger);
    }
    return strategy;
  }

  public void writeConfiguration() {
    try {
      final File f = new File(OSystemVariableResolver.resolveSystemVariables(configFile));
      OIOUtils.writeFile(f, configuration.toJSON("prettyPrint"));
    } catch (IOException e) {
      OException.wrapException(new OConfigurationException("Cannot save Backup configuration file '" + configFile + "'. "), e);
    }
  }

}

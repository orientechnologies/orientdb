/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.server.handler;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.parser.OVariableParser;
import com.orientechnologies.common.parser.OVariableParserListener;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Automatically creates a backup at configured time. Starting from v2.2, this component is able
 * also to create incremental backup and export of databases. If you need a mix of different modes,
 * configure more instances of the same component.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OAutomaticBackup implements OServerPlugin {
  private static final OLogger logger = OLogManager.instance().logger(OAutomaticBackup.class);

  private Set<OAutomaticBackupListener> listeners =
      Collections.newSetFromMap(new ConcurrentHashMap<OAutomaticBackupListener, Boolean>());

  public enum VARIABLES {
    DBNAME,
    DATE
  }

  public enum MODE {
    FULL_BACKUP,
    INCREMENTAL_BACKUP,
    EXPORT
  }

  private String configFile = "${ORIENTDB_HOME}/config/automatic-backup.json";

  private OServer serverInstance;
  private OAtomaticBackupConfig config;

  @Override
  public void config(final OServer iServer, final OServerParameterConfiguration[] iParams) {
    serverInstance = iServer;

    if (iParams.length != 0) {
      for (OServerParameterConfiguration param : iParams) {
        if (param.getName().equalsIgnoreCase("config") && param.getValue().trim().length() > 0) {
          configFile = param.getValue().trim();

          final File f = new File(OSystemVariableResolver.resolveSystemVariables(configFile));
          if (!f.exists())
            throw new OConfigurationException(
                "Automatic Backup configuration file '"
                    + configFile
                    + "' not found. Automatic Backup will be disabled");
          try {
            final String configurationContent = OIOUtils.readFileAsString(f);
            config =
                OAtomaticBackupConfig.newFromODocument(
                    new ODocument().fromJSON(configurationContent));
          } catch (IOException e) {
            throw OException.wrapException(
                new OConfigurationException(
                    "Cannot load Automatic Backup configuration file '"
                        + configFile
                        + "'. Automatic Backup will be disabled"),
                e);
          }
        }
      }
      if (config == null) {
        config = OAtomaticBackupConfig.newFromParams(iParams);
        if (config != null) {
          final File f = new File(OSystemVariableResolver.resolveSystemVariables(configFile));
          if (!f.exists()) {
            try {
              f.getParentFile().mkdirs();
              f.createNewFile();
              OIOUtils.writeFile(f, config.toDocument().toJSON("prettyPrint"));

              logger.info("Automatic Backup: migrated configuration to file '%s'", f);
            } catch (IOException e) {
              throw OException.wrapException(
                  new OConfigurationException(
                      "Cannot create Automatic Backup configuration file '"
                          + configFile
                          + "'. Automatic Backup will be disabled"),
                  e);
            }
          }
        }
      }

    } else {
      final File f = new File(OSystemVariableResolver.resolveSystemVariables(configFile));
      if (f.exists()) {
        // READ THE FILE
        try {
          final String configurationContent = OIOUtils.readFileAsString(f);
          config =
              OAtomaticBackupConfig.newFromODocument(
                  new ODocument().fromJSON(configurationContent));
        } catch (IOException e) {
          throw OException.wrapException(
              new OConfigurationException(
                  "Cannot load Automatic Backup configuration file '"
                      + configFile
                      + "'. Automatic Backup will be disabled"),
              e);
        }
      }
    }

    if (config != null && config.isEnabled()) {
      if (config.getDelay() <= 0)
        throw new OConfigurationException("Cannot find mandatory parameter 'delay'");

      final File filePath = config.getTargetDirectory().toFile();
      if (filePath.exists()) {
        if (!filePath.isDirectory())
          throw new OConfigurationException("Parameter 'path' points to a file, not a directory");
      } else
        // CREATE BACKUP FOLDER(S) IF ANY
        filePath.mkdirs();

      logger.info(
          "Automatic Backup plugin installed and active: delay=%dms, firstTime=%s,"
              + " targetDirectory=%s",
          config.getDelay(), config.getFirstTime(), config.getTargetDirectory());

      OrientDBInternal ctx = serverInstance.getDatabases();
      if (config.getFirstTime() == null) {
        ctx.periodicExecute(this::executeBackup, config.getDelay());
      } else {
        ctx.scheduleExecuteFrom(this::executeBackup, config.getFirstTime(), config.getDelay());
      }
    } else {
      logger.info("Automatic Backup plugin is disabled");
    }
  }

  private void executeBackup() {
    logger.info("Scanning databases to backup...");

    int ok = 0;
    int errors = 0;

    OrientDBInternal ctx = serverInstance.getDatabases();
    final Set<String> databases = ctx.listDatabases(null, null);
    for (String dbName : databases) {

      boolean include;

      if (config.getDbInclude().size() > 0) include = config.getDbInclude().contains(dbName);
      else include = true;

      if (config.getDbExclude().contains(dbName)) include = false;

      if (include) {
        ODatabaseDocumentInternal db = null;
        try {
          db = ctx.openNoAuthorization(dbName);

          final long begin = System.currentTimeMillis();

          switch (config.getMode()) {
            case FULL_BACKUP:
              fullBackupDatabase(
                  dbName, config.getTargetDirectory().resolve(getFileName(dbName)).toString(), db);

              logger.info(
                  "Full Backup of database '%s' completed in %d ms",
                  dbName, (System.currentTimeMillis() - begin));

              break;

            case INCREMENTAL_BACKUP:
              incrementalBackupDatabase(dbName, config.getTargetDirectory().toString(), db);

              logger.info(
                  "Incremental Backup of database '%s' completed in %d ms",
                  dbName, (System.currentTimeMillis() - begin));
              break;

            case EXPORT:
              exportDatabase(
                  dbName, config.getTargetDirectory().resolve(getFileName(dbName)).toString(), db);

              logger.info(
                  "Export of database '%s' completed in %d ms",
                  dbName, (System.currentTimeMillis() - begin));
              break;
          }

          try {

            for (OAutomaticBackupListener listener : listeners) {
              listener.onBackupCompleted(dbName);
            }
          } catch (Exception e) {
            logger.error("Error on listener for database '%s'", e, dbName);
          }
          ok++;

        } catch (Exception e) {

          logger.error(
              "Error on backup of database '%s' to directory: %s",
              e, dbName, config.getTargetDirectory());

          try {
            for (OAutomaticBackupListener listener : listeners) {
              listener.onBackupError(dbName, e);
            }
          } catch (Exception l) {
            logger.error("Error on listener for database '%s'", l, dbName);
          }
          errors++;

        } finally {
          if (db != null) db.close();
        }
      }
    }
    logger.info("Automatic Backup finished: %d ok, %d errors", ok, errors);
  }

  protected void incrementalBackupDatabase(
      final String dbURL, String iPath, final ODatabaseDocumentInternal db) throws IOException {
    // APPEND DB NAME TO THE DIRECTORY NAME
    if (!iPath.endsWith("/")) iPath += "/";
    iPath += db.getName();

    logger.info(
        "AutomaticBackup: executing incremental backup of database '%s' to %s", dbURL, iPath);

    db.incrementalBackup(iPath);
  }

  protected void fullBackupDatabase(
      final String dbName, final String iPath, final ODatabaseDocumentInternal db)
      throws IOException {
    logger.info("AutomaticBackup: executing full backup of database '%s' to %s", dbName, iPath);

    final Path filePath = Paths.get(iPath);
    OFileUtils.prepareForFileCreationOrReplacement(filePath, this, "backing up");

    final String tempFileName = iPath + ".tmp";
    final Path tempFilePath = Paths.get(tempFileName);
    OFileUtils.prepareForFileCreationOrReplacement(tempFilePath, this, "backing up");

    try {
      try (FileOutputStream fileOutputStream = new FileOutputStream(tempFileName)) {
        db.backup(
            fileOutputStream,
            null,
            null,
            new OCommandOutputListener() {
              @Override
              public void onMessage(String iText) {
                logger.info("%s", iText);
              }
            },
            config.getCompressionLevel(),
            config.getBufferSize());
      }

      OFileUtils.atomicMoveWithFallback(tempFilePath, filePath, this);
    } catch (Exception e) {
      logger.errorNoDb(
          "Error during backup processing, file %s will be deleted\n", e, tempFileName);
      Files.deleteIfExists(tempFilePath);
      throw e;
    }
  }

  protected void exportDatabase(
      final String dbName, final String iPath, final ODatabaseDocumentInternal db)
      throws IOException {

    logger.info("AutomaticBackup: executing export of database '%s' to %s", dbName, iPath);

    final ODatabaseExport exp =
        new ODatabaseExport(
            db,
            iPath,
            new OCommandOutputListener() {
              @Override
              public void onMessage(String iText) {
                logger.info("%s", iText);
              }
            });

    if (config.getExportOptions() != null && !config.getExportOptions().trim().isEmpty())
      exp.setOptions(config.getExportOptions().trim());

    exp.exportDatabase().close();
  }

  protected String getFileName(String dbName) {
    return (String)
        OVariableParser.resolveVariables(
            config.getTargetFileName(),
            OSystemVariableResolver.VAR_BEGIN,
            OSystemVariableResolver.VAR_END,
            new OVariableParserListener() {
              @Override
              public String resolve(final String iVariable) {
                if (iVariable.equalsIgnoreCase(VARIABLES.DBNAME.toString())) return dbName;
                else if (iVariable.startsWith(VARIABLES.DATE.toString())) {
                  return new SimpleDateFormat(
                          iVariable.substring(VARIABLES.DATE.toString().length() + 1))
                      .format(new Date());
                }

                // NOT FOUND
                throw new IllegalArgumentException("Variable '" + iVariable + "' was not found");
              }
            });
  }

  @Override
  public String getName() {
    return "automaticBackup";
  }

  public void registerListener(OAutomaticBackupListener listener) {
    listeners.add(listener);
  }

  public void unregisterListener(OAutomaticBackupListener listener) {
    listeners.remove(listener);
  }

  public interface OAutomaticBackupListener {
    void onBackupCompleted(String database);

    void onBackupError(String database, Exception e);
  }
}

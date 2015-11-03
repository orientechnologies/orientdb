/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.server.handler;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.parser.OVariableParser;
import com.orientechnologies.common.parser.OVariableParserListener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.security.OSecurityNull;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimerTask;

/**
 * Automatically creates a backup at configured time. Starting from v2.2, this component is able also to create incremental backup
 * and export of databases. If you need a mix of different modes, configure more instances of the same component.
 * 
 * @author Luca Garulli
 */
public class OAutomaticBackup extends OServerPluginAbstract {

  public enum VARIABLES {
    DBNAME, DATE
  }

  public enum MODE {
    FULL_BACKUP, INCREMENTAL_BACKUP, EXPORT
  }

  private Date   firstTime        = null;
  private long   delay            = -1;
  private int    bufferSize       = 1048576;
  private int    compressionLevel = 9;
  private MODE   mode             = MODE.FULL_BACKUP;
  private String exportOptions;

  private String      targetDirectory  = "backup";
  private String      targetFileName;
  private Set<String> includeDatabases = new HashSet<String>();
  private Set<String> excludeDatabases = new HashSet<String>();
  private OServer     serverInstance;

  @Override
  public void config(final OServer iServer, final OServerParameterConfiguration[] iParams) {
    serverInstance = iServer;

    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("enabled")) {
        if (!Boolean.parseBoolean(param.value))
          // DISABLE IT
          return;
      } else if (param.name.equalsIgnoreCase("delay"))
        delay = OIOUtils.getTimeAsMillisecs(param.value);
      else if (param.name.equalsIgnoreCase("firstTime")) {
        try {
          firstTime = OIOUtils.getTodayWithTime(param.value);
          if (firstTime.before(new Date())) {
            Calendar cal = Calendar.getInstance();
            cal.setTime(firstTime);
            cal.add(Calendar.DAY_OF_MONTH, 1);
            firstTime = cal.getTime();
          }
        } catch (ParseException e) {
          throw OException
              .wrapException(new OConfigurationException("Parameter 'firstTime' has invalid format, expected: HH:mm:ss"), e);
        }
      } else if (param.name.equalsIgnoreCase("target.directory"))
        targetDirectory = param.value;
      else if (param.name.equalsIgnoreCase("db.include") && param.value.trim().length() > 0)
        for (String db : param.value.split(","))
          includeDatabases.add(db);
      else if (param.name.equalsIgnoreCase("db.exclude") && param.value.trim().length() > 0)
        for (String db : param.value.split(","))
          excludeDatabases.add(db);
      else if (param.name.equalsIgnoreCase("target.fileName"))
        targetFileName = param.value;
      else if (param.name.equalsIgnoreCase("bufferSize"))
        bufferSize = Integer.parseInt(param.value);
      else if (param.name.equalsIgnoreCase("compressionLevel"))
        compressionLevel = Integer.parseInt(param.value);
      else if (param.name.equalsIgnoreCase("mode"))
        mode = MODE.valueOf(param.value.toUpperCase());
      else if (param.name.equalsIgnoreCase("exportOptions"))
        exportOptions = param.value;
    }

    if (delay <= 0)
      throw new OConfigurationException("Cannot find mandatory parameter 'delay'");
    if (!targetDirectory.endsWith("/"))
      targetDirectory += "/";

    final File filePath = new File(targetDirectory);
    if (filePath.exists()) {
      if (!filePath.isDirectory())
        throw new OConfigurationException("Parameter 'path' points to a file, not a directory");
    } else
      // CREATE BACKUP FOLDER(S) IF ANY
      filePath.mkdirs();

    OLogManager.instance().info(this, "Automatic backup plugin installed and active: delay=%dms, firstTime=%s, targetDirectory=%s",
        delay, firstTime, targetDirectory);

    final TimerTask timerTask = new TimerTask() {
      @Override
      public void run() {
        OLogManager.instance().info(this, "Scanning databases to backup...");

        int ok = 0, errors = 0;

        final Map<String, String> databases = serverInstance.getAvailableStorageNames();
        for (final Entry<String, String> database : databases.entrySet()) {
          final String dbName = database.getKey();
          final String dbURL = database.getValue();

          boolean include;

          if (includeDatabases.size() > 0)
            include = includeDatabases.contains(dbName);
          else
            include = true;

          if (excludeDatabases.contains(dbName))
            include = false;

          if (include) {
            ODatabaseDocumentTx db = null;
            try {

              db = new ODatabaseDocumentTx(dbURL);
              db.setProperty(ODatabase.OPTIONS.SECURITY.toString(), OSecurityNull.class);
              db.open("admin", "aaa");

              final long begin = System.currentTimeMillis();

              switch (mode) {
              case FULL_BACKUP:
                fullBackupDatabase(dbURL, targetDirectory + getFileName(database), db);

                OLogManager.instance().info(this,
                    "Full Backup of database '" + dbURL + "' completed in " + (System.currentTimeMillis() - begin) + "ms");

                break;

              case INCREMENTAL_BACKUP:
                incrementalBackupDatabase(dbURL, targetDirectory, db);

                OLogManager.instance().info(this,
                    "Incremental Backup of database '" + dbURL + "' completed in " + (System.currentTimeMillis() - begin) + "ms");
                break;

              case EXPORT:
                exportDatabase(dbURL, targetDirectory + getFileName(database), db);

                OLogManager.instance().info(this,
                    "Export of database '" + dbURL + "' completed in " + (System.currentTimeMillis() - begin) + "ms");
                break;
              }

              ok++;

            } catch (Exception e) {

              OLogManager.instance().error(this, "Error on backup of database '" + dbURL + "' to directory: " + targetDirectory, e);
              errors++;

            } finally {
              if (db != null)
                db.close();
            }
          }
        }
        OLogManager.instance().info(this, "Automatic Backup finished: %d ok, %d errors", ok, errors);
      }
    };

    if (firstTime == null)
      Orient.instance().scheduleTask(timerTask, delay, delay);
    else
      Orient.instance().scheduleTask(timerTask, firstTime, delay);
  }

  protected void incrementalBackupDatabase(final String dbURL, String iPath, final ODatabaseDocumentTx db) throws IOException {
    // APPEND DB NAME TO THE DIRECTORY NAME
    if (!iPath.endsWith("/"))
      iPath += "/";
    iPath += db.getName();

    db.incrementalBackup(iPath);
  }

  protected void fullBackupDatabase(final String dbURL, final String iPath, final ODatabaseDocumentTx db) throws IOException {
    db.backup(new FileOutputStream(iPath), null, null, new OCommandOutputListener() {
      @Override
      public void onMessage(String iText) {
        OLogManager.instance().info(this, iText);
      }
    }, compressionLevel, bufferSize);
  }

  protected void exportDatabase(final String dbURL, final String iPath, final ODatabaseDocumentTx db) throws IOException {

    final ODatabaseExport exp = new ODatabaseExport(db, iPath, new OCommandOutputListener() {
      @Override
      public void onMessage(String iText) {
        OLogManager.instance().info(this, iText);
      }
    });

    if (exportOptions != null && !exportOptions.trim().isEmpty())
      exp.setOptions(exportOptions.trim());

    exp.exportDatabase().close();
  }

  protected String getFileName(final Entry<String, String> dbName) {
    return (String) OVariableParser.resolveVariables(targetFileName, OSystemVariableResolver.VAR_BEGIN,
        OSystemVariableResolver.VAR_END, new OVariableParserListener() {
          @Override
          public String resolve(final String iVariable) {
            if (iVariable.equalsIgnoreCase(VARIABLES.DBNAME.toString()))
              return dbName.getKey();
            else if (iVariable.startsWith(VARIABLES.DATE.toString())) {
              return new SimpleDateFormat(iVariable.substring(VARIABLES.DATE.toString().length() + 1)).format(new Date());
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
}

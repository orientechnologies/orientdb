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

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.common.parser.OVariableParser;
import com.orientechnologies.common.parser.OVariableParserListener;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.security.OSecurityNull;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPluginAbstract;

import java.io.File;
import java.io.FileOutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

public class OAutomaticBackup extends OServerPluginAbstract {

  public enum VARIABLES {
    DBNAME, DATE
  }

  private Date        firstTime        = null;
  private long        delay            = -1;
  private int         bufferSize       = 1048576;
  private int         compressionLevel = 9;
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
          throw new OConfigurationException("Parameter 'firstTime' has invalid format, expected: HH:mm:ss", e);
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
        OLogManager.instance().info(this, "[OAutomaticBackup] Scanning databases to backup...");

        int ok = 0, errors = 0;

        final Map<String, String> databaseNames = serverInstance.getAvailableStorageNames();
        for (final Entry<String, String> dbName : databaseNames.entrySet()) {
          boolean include;

          if (includeDatabases.size() > 0)
            include = includeDatabases.contains(dbName.getKey());
          else
            include = true;

          if (excludeDatabases.contains(dbName.getKey()))
            include = false;

          if (include) {
            final String fileName = (String) OVariableParser.resolveVariables(targetFileName, OSystemVariableResolver.VAR_BEGIN,
                OSystemVariableResolver.VAR_END, new OVariableParserListener() {
              @Override
              public String resolve(final String iVariable) {
                if (iVariable.equalsIgnoreCase(VARIABLES.DBNAME.toString()))
                  return dbName.getKey();
                else if (iVariable.startsWith(VARIABLES.DATE.toString())) {
                  return new SimpleDateFormat(iVariable.substring(VARIABLES.DATE.toString().length() + 1)).format(new Date());
                }

                // NOT FOUND
                throw new IllegalArgumentException("Variable '" + iVariable + "' wasn't found");
              }
            });

            final String exportFilePath = targetDirectory + fileName;
            ODatabaseDocumentTx db = null;
            try {

              db = new ODatabaseDocumentTx(dbName.getValue());
              db.setProperty(ODatabase.OPTIONS.SECURITY.toString(), OSecurityNull.class);
              db.open("admin", "aaa");

              final long begin = System.currentTimeMillis();

              db.backup(new FileOutputStream(exportFilePath), null, null, new OCommandOutputListener() {
                @Override
                public void onMessage(String iText) {
                  OLogManager.instance().info(this, iText);
                }
              }, compressionLevel, bufferSize);

              OLogManager.instance().info(this, "[OAutomaticBackup] - Backup of database '" + dbName.getValue() + "' completed in "
                  + (System.currentTimeMillis() - begin) + "ms");
              ok++;

            } catch (Exception e) {

              OLogManager.instance().error(this,
                  "[OAutomaticBackup] - Error on exporting database '" + dbName.getValue() + "' to file: " + exportFilePath, e);
              errors++;

            } finally {
              if (db != null)
                db.close();
            }
          }
        }
        OLogManager.instance().info(this, "[OAutomaticBackup] Backup finished: %d ok, %d errors", ok, errors);
      }
    };

    if (firstTime == null)
      Orient.instance().scheduleTask(timerTask, delay, delay);
    else
      Orient.instance().scheduleTask(timerTask, firstTime, delay);
  }

  @Override
  public String getName() {
    return "automaticBackup";
  }
}

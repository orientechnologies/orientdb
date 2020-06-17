/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
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

package com.orientechnologies.orient.etl.util;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.context.OETLContext;
import com.orientechnologies.orient.etl.context.OETLContextWrapper;
import java.io.File;
import java.io.IOException;

/**
 * @author Gabriele Ponzi
 * @email <g.ponzi--at--orientdb.com>
 */
public class OMigrationConfigManager {

  // config info
  private static final String configurationDirectoryName = "etl-config/";
  private static final String configFileDefaultName = "migration-config.json"; // path
  // ORIENTDB_HOME/<db-name>/teleporter-config/migration-config.json

  /**
   * @param migrationConfig
   * @param outOrientGraphUri
   * @param configName
   */
  public static String writeConfigurationInTargetDB(
      ODocument migrationConfig, String outOrientGraphUri, String configName) {

    String outDBConfigPath = prepareConfigDirectoryForWriting(outOrientGraphUri, configName);
    String jsonSourcesInfo = migrationConfig.toJSON("prettyPrint");
    try {
      OFileManager.writeFileFromText(jsonSourcesInfo, outDBConfigPath, false);
    } catch (IOException e) {
      String mess = "";
      ((OETLContext) OETLContextWrapper.getInstance().getContext())
          .printExceptionMessage(e, mess, "error");
      ((OETLContext) OETLContextWrapper.getInstance().getContext())
          .printExceptionStackTrace(e, "error");
    }

    return outDBConfigPath;
  }

  /**
   * @param migrationConfig
   * @param outOrientGraphUri
   * @param configName
   * @return Configuration file path.
   * @throws IOException
   */
  public static String writeConfigurationInTargetDB(
      String migrationConfig, String outOrientGraphUri, String configName) throws IOException {

    String outDBConfigPath = prepareConfigDirectoryForWriting(outOrientGraphUri, configName);
    OFileManager.writeFileFromText(migrationConfig, outDBConfigPath, false);

    return outDBConfigPath;
  }

  private static String prepareConfigDirectoryForWriting(
      String outOrientGraphUri, String configName) {

    String outDBConfigPath;
    if (configName == null) {
      outDBConfigPath = buildConfigurationFilePath(outOrientGraphUri, configFileDefaultName);
    } else {
      if (!configName.endsWith(".json")) {
        configName += ".json";
      }
      outDBConfigPath = buildConfigurationFilePath(outOrientGraphUri, configName);
    }
    File confFileInOrientDB = new File(outDBConfigPath);

    if (confFileInOrientDB.exists()) {
      confFileInOrientDB.delete();
    }
    return outDBConfigPath;
  }

  public static String buildConfigurationFilePath(String outOrientGraphUri, String configFileName) {
    if (outOrientGraphUri.contains("\\")) {
      outOrientGraphUri = outOrientGraphUri.replace("\\", "/");
    }

    if (!configFileName.endsWith(".json")) {
      configFileName += ".json";
    }

    // checking the presence of the migrationConfigDoc in the target db
    if (!(outOrientGraphUri.charAt(outOrientGraphUri.length() - 1) == '/')) {
      outOrientGraphUri += "/";
    }
    String outDBConfigPath = outOrientGraphUri + configurationDirectoryName + configFileName;
    outDBConfigPath = outDBConfigPath.replace("plocal:", "");
    return outDBConfigPath;
  }
}

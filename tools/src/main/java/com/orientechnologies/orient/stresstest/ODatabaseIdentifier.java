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
package com.orientechnologies.orient.stresstest;

import java.io.File;

/**
 * A class that contains all the info needed to access a database
 *
 * @author Andrea Iacono
 */
public class ODatabaseIdentifier {

  private OStressTesterSettings settings;

  public ODatabaseIdentifier(final OStressTesterSettings settings) {
    this.settings = settings;
  }

  public String getUrl() {

    switch (settings.mode) {
      case MEMORY:
        return "memory:" + settings.dbName;
      case REMOTE:
        return "remote:" + settings.remoteIp + ":" + settings.remotePort + "/" + settings.dbName;
      case DISTRIBUTED:
        return null;
      case PLOCAL:
      default:
        String basePath = System.getProperty("java.io.tmpdir");
        if (settings.plocalPath != null) {
          basePath = settings.plocalPath;
        }

        if (!basePath.endsWith(File.separator)) basePath += File.separator;

        return "plocal:" + basePath + settings.dbName;
    }
  }

  public OStressTester.OMode getMode() {
    return settings.mode;
  }

  public String getPassword() {
    return settings.rootPassword;
  }

  public void setPassword(String password) {
    settings.rootPassword = password;
  }

  public String getName() {
    return settings.dbName;
  }

  public String getRemoteIp() {
    return settings.remoteIp;
  }

  public int getRemotePort() {
    return settings.remotePort;
  }

  public String getPlocalPath() {
    return settings.plocalPath;
  }
}

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
package com.orientechnologies.orient.server.config;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Server configuration manager. It manages the orientdb-server-config.xml file.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OServerConfigurationManager {
  private final OServerConfigurationLoaderXml configurationLoader;
  private OServerConfiguration configuration;

  public OServerConfigurationManager(final InputStream iInputStream) throws IOException {
    configurationLoader =
        new OServerConfigurationLoaderXml(OServerConfiguration.class, iInputStream);
    configuration = configurationLoader.load();
  }

  public OServerConfigurationManager(final File iFile) throws IOException {
    configurationLoader = new OServerConfigurationLoaderXml(OServerConfiguration.class, iFile);
    configuration = configurationLoader.load();
  }

  public OServerConfigurationManager(final OServerConfiguration iConfiguration) {
    configurationLoader = null;
    configuration = iConfiguration;
  }

  public OServerConfiguration getConfiguration() {
    return configuration;
  }

  public OServerConfigurationManager setUser(
      final String iServerUserName, final String iServerUserPasswd, final String iPermissions) {
    if (iServerUserName == null || iServerUserName.length() == 0)
      throw new IllegalArgumentException("User name is null or empty");

    // An empty password is permissible as some security implementations do not require it.
    if (iServerUserPasswd == null)
      throw new IllegalArgumentException("User password is null or empty");

    if (iPermissions == null || iPermissions.length() == 0)
      throw new IllegalArgumentException("User permissions is null or empty");

    int userPositionInArray = -1;

    if (configuration.users == null) {
      configuration.users = new OServerUserConfiguration[1];
      userPositionInArray = 0;
    } else {
      // LOOK FOR EXISTENT USER
      for (int i = 0; i < configuration.users.length; ++i) {
        final OServerUserConfiguration u = configuration.users[i];

        if (u != null && iServerUserName.equalsIgnoreCase(u.name)) {
          // FOUND
          userPositionInArray = i;
          break;
        }
      }

      if (userPositionInArray == -1) {
        // NOT FOUND
        userPositionInArray = configuration.users.length;
        configuration.users = Arrays.copyOf(configuration.users, configuration.users.length + 1);
      }
    }

    configuration.users[userPositionInArray] =
        new OServerUserConfiguration(iServerUserName, iServerUserPasswd, iPermissions);

    return this;
  }

  public void saveConfiguration() throws IOException {
    if (configurationLoader == null) return;

    configurationLoader.save(configuration);
  }

  public OServerUserConfiguration getUser(final String iServerUserName) {
    if (iServerUserName == null || iServerUserName.length() == 0) {
      throw new IllegalArgumentException("User name is null or empty");
    }

    checkForAutoReloading();

    if (configuration.users != null) {
      for (OServerUserConfiguration user : configuration.users) {
        if (iServerUserName.equalsIgnoreCase(user.name)) {
          // FOUND
          return user;
        }
      }
    }

    return null;
  }

  public boolean existsUser(final String iServerUserName) {
    return getUser(iServerUserName) != null;
  }

  public void dropUser(final String iServerUserName) {
    if (iServerUserName == null || iServerUserName.length() == 0) {
      throw new IllegalArgumentException("User name is null or empty");
    }

    checkForAutoReloading();

    // LOOK FOR EXISTENT USER
    for (int i = 0; i < configuration.users.length; ++i) {
      final OServerUserConfiguration u = configuration.users[i];

      if (u != null && iServerUserName.equalsIgnoreCase(u.name)) {
        // FOUND
        final OServerUserConfiguration[] newArray =
            new OServerUserConfiguration[configuration.users.length - 1];
        // COPY LEFT PART
        for (int k = 0; k < i; ++k) {
          newArray[k] = configuration.users[k];
        }
        // COPY RIGHT PART
        for (int k = i; k < newArray.length; ++k) {
          newArray[k] = configuration.users[k + 1];
        }
        configuration.users = newArray;
        break;
      }
    }
  }

  public Set<OServerUserConfiguration> getUsers() {
    checkForAutoReloading();

    final HashSet<OServerUserConfiguration> result = new HashSet<OServerUserConfiguration>();
    if (configuration.users != null) {
      for (int i = 0; i < configuration.users.length; ++i) {
        if (configuration.users[i] != null) result.add(configuration.users[i]);
      }
    }

    return result;
  }

  private void checkForAutoReloading() {
    if (configurationLoader != null)
      if (configurationLoader.checkForAutoReloading()) {
        try {
          configuration = configurationLoader.load();
        } catch (IOException e) {
          throw OException.wrapException(
              new OConfigurationException("Cannot load server configuration"), e);
        }
      }
  }
}

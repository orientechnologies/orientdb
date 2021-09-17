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

import com.orientechnologies.orient.server.config.distributed.OServerDistributedConfiguration;
import jakarta.xml.bind.annotation.XmlElementRef;
import jakarta.xml.bind.annotation.XmlElementWrapper;
import jakarta.xml.bind.annotation.XmlRootElement;
import jakarta.xml.bind.annotation.XmlTransient;
import java.util.List;

@XmlRootElement(name = "orient-server")
public class OServerConfiguration {
  public static final String FILE_NAME = "server-config.xml";
  // private static final String HEADER = "OrientDB Server configuration";
  public static final OServerStorageConfiguration[] EMPTY_CONFIG_ARRAY =
      new OServerStorageConfiguration[0];
  @XmlTransient public String location;

  @XmlElementWrapper
  @XmlElementRef(type = OServerHandlerConfiguration.class)
  public List<OServerHandlerConfiguration> handlers;

  @XmlElementWrapper
  @XmlElementRef(type = OServerHookConfiguration.class)
  public List<OServerHookConfiguration> hooks;

  @XmlElementRef(type = OServerNetworkConfiguration.class)
  public OServerNetworkConfiguration network;

  @XmlElementWrapper
  @XmlElementRef(type = OServerStorageConfiguration.class)
  public OServerStorageConfiguration[] storages;

  @XmlElementWrapper(required = false)
  @XmlElementRef(type = OServerUserConfiguration.class)
  public OServerUserConfiguration[] users;

  @XmlElementRef(type = OServerSecurityConfiguration.class)
  public OServerSecurityConfiguration security;

  @XmlElementWrapper
  @XmlElementRef(type = OServerEntryConfiguration.class)
  public OServerEntryConfiguration[] properties;

  @XmlElementRef(type = OServerDistributedConfiguration.class)
  public OServerDistributedConfiguration distributed;

  public boolean isAfterFirstTime;

  public static final String DEFAULT_CONFIG_FILE = "config/orientdb-server-config.xml";

  public static final String PROPERTY_CONFIG_FILE = "orientdb.config.file";

  public static final String DEFAULT_ROOT_USER = "root";
  public static final String GUEST_USER = "guest";
  public static final String DEFAULT_GUEST_PASSWORD = "!!!TheGuestPw123";

  /** Empty constructor for JAXB */
  public OServerConfiguration() {}

  public OServerConfiguration(OServerConfigurationLoaderXml iFactory) {
    location = FILE_NAME;
    network = new OServerNetworkConfiguration(iFactory);
    storages = EMPTY_CONFIG_ARRAY;
    security = new OServerSecurityConfiguration(iFactory);
  }

  public String getStoragePath(String iURL) {
    if (storages != null)
      for (OServerStorageConfiguration stg : storages) if (stg.name.equals(iURL)) return stg.path;

    return null;
  }

  /**
   * Returns the property value configured, if any.
   *
   * @param iName Property name to find
   */
  public String getProperty(final String iName) {
    return getProperty(iName, null);
  }

  /**
   * Returns the property value configured, if any.
   *
   * @param iName Property name to find
   * @param iDefaultValue Default value returned if not found
   */
  public String getProperty(final String iName, final String iDefaultValue) {
    if (properties == null) return null;

    for (OServerEntryConfiguration p : properties) {
      if (p.name.equals(iName)) return p.value;
    }

    return null;
  }
}

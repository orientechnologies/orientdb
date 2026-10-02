/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>For more information: http://www.orientdb.com
 */
package com.orientechnologies.security;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.security.auditing.ODefaultAuditing;
import com.orientechnologies.security.kerberos.OKerberosAuthenticator;
import com.orientechnologies.security.ldap.OLDAPImporter;
import com.orientechnologies.security.password.ODefaultPasswordValidator;

public class OSecurityPlugin implements OServerPlugin {
  private static final OLogger logger = OLogManager.instance().logger(OSecurityPlugin.class);
  private OrientDBInternal context;

  @Override
  public void config(OServer server, OServerParameterConfiguration[] iParams) {
    this.context = server.getDatabases();
  }

  @Override
  public String getName() {
    return "security-plugin";
  }

  @Override
  public void startup() {
    registerSecurityComponents();
  }

  @Override
  public void shutdown() {
    unregisterSecurityComponents();
  }

  // The OSecurityModule resides in the main application's class loader. Its configuration file
  // may reference components that are reside in pluggable modules.
  // A security plugin should register its components so that OSecuritySystem has access to them.
  private void registerSecurityComponents() {
    try {
      if (context.getSecuritySystem() != null) {
        context.getSecuritySystem().registerSecurityClass(ODefaultAuditing.class);
        context.getSecuritySystem().registerSecurityClass(ODefaultPasswordValidator.class);
        context.getSecuritySystem().registerSecurityClass(OKerberosAuthenticator.class);
        context.getSecuritySystem().registerSecurityClass(OLDAPImporter.class);
      }
    } catch (Throwable th) {
      logger.error("registerSecurityComponents() ", th);
    }
  }

  private void unregisterSecurityComponents() {
    try {
      if (context.getSecuritySystem() != null) {
        context.getSecuritySystem().unregisterSecurityClass(ODefaultAuditing.class);
        context.getSecuritySystem().unregisterSecurityClass(ODefaultPasswordValidator.class);
        context.getSecuritySystem().unregisterSecurityClass(OKerberosAuthenticator.class);
        context.getSecuritySystem().unregisterSecurityClass(OLDAPImporter.class);
      }
    } catch (Throwable th) {
      logger.error("unregisterSecurityComponents()", th);
    }
  }
}

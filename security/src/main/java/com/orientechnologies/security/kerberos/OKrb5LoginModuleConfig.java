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
package com.orientechnologies.security.kerberos;

import java.util.HashMap;
import java.util.Map;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

/**
 * Custom Kerberos login configuration.
 *
 * @author S. Colin Leister
 */
public class OKrb5LoginModuleConfig extends Configuration {
  final String loginModule = "com.sun.security.auth.module.Krb5LoginModule";

  private final AppConfigurationEntry[] appConfigEntries = new AppConfigurationEntry[1];

  public AppConfigurationEntry[] getAppConfigurationEntry(String applicationName) {
    return appConfigEntries;
  }

  public OKrb5LoginModuleConfig(String principal, String keyTabPath) {
    if (keyTabPath != null) {
      final Map<String, Object> options = new HashMap<String, Object>();

      // If isInitiator is true, then acquiring a TGT is mandatory.
      options.put("isInitiator", "false");

      options.put("principal", principal);

      options.put("useKeyTab", "true");
      options.put("keyTab", keyTabPath);

      // storeKey is essential or else you'll get an "Invalid argument (400) - Cannot find key of
      // appropriate type to decrypt AP REP" in your acceptSecContext() call.
      options.put("storeKey", "true");

      options.put("doNotPrompt", "true");

      appConfigEntries[0] =
          new AppConfigurationEntry(
              loginModule, AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options);
    }
  }
}

/*
 *
 *  *  Copyright 2015 OrientDB LTD (info(-at-)orientdb.com)
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
package com.orientechnologies.orient.core.security.kerberos;

import java.util.HashMap;
import java.util.Map;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

/**
 * Custom Kerberos client login configuration.
 *
 * @author S. Colin Leister
 */
public class OKrb5ClientLoginModuleConfig extends Configuration {
  private final String loginModule = "com.sun.security.auth.module.Krb5LoginModule";

  private final AppConfigurationEntry[] appConfigEntries = new AppConfigurationEntry[1];

  public AppConfigurationEntry[] getAppConfigurationEntry(String applicationName) {
    return appConfigEntries;
  }
  /*
  	public OKrb5ClientLoginModuleConfig(String ccPath)
  	{
  		if(ccPath != null)
  		{
  			final Map<String, Object> options = new HashMap<String, Object>();

  			options.put("useTicketCache", "true");
  			options.put("ticketCache", ccPath);
  			options.put("doNotPrompt", "true");

  			_appConfigEntries[0] = new AppConfigurationEntry(LoginModule, AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options);
  		}
  	}
  */

  public OKrb5ClientLoginModuleConfig(String principal, String ccPath, String ktPath) {
    this(principal, true, ccPath, ktPath);
  }

  public OKrb5ClientLoginModuleConfig(
      String principal, boolean useTicketCache, String ccPath, String ktPath) {
    final Map<String, Object> options = new HashMap<String, Object>();

    options.put("principal", principal);

    // This is the default, but let's be specific.
    // If isInitiator is true, then acquiring a TGT is mandatory.
    // If we're using a valid ticket cache then we should already have a TGT and this is technically
    // not needed.
    // If not, and we use the keytab for authentication, then we'll have to acquire a TGT.
    options.put("isInitiator", "true");

    if (ccPath != null && ccPath.length() > 0) {
      if (useTicketCache) {
        options.put("useTicketCache", "true");
        options.put("ticketCache", ccPath);
      } else {
        options.put("useTicketCache", "false");
      }
    }

    if (ktPath != null && ktPath.length() > 0) {
      options.put("useKeyTab", "true");
      options.put("keyTab", ktPath);

      // storeKey is essential or else you'll get an "Invalid argument (400) - Cannot find key of
      // appropriate type to decrypt AP REP" in your acceptSecContext() call.
      options.put("storeKey", "true");
    }

    options.put("doNotPrompt", "true");

    appConfigEntries[0] =
        new AppConfigurationEntry(
            loginModule, AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options);
  }
}

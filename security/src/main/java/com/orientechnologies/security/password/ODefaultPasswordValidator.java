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
package com.orientechnologies.security.password;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.security.OInvalidPasswordException;
import com.orientechnologies.orient.core.security.OPasswordValidator;
import com.orientechnologies.orient.core.security.OSecuritySystem;
import java.util.regex.Pattern;

/**
 * Provides a default implementation for validating passwords.
 *
 * @author S. Colin Leister
 */
public class ODefaultPasswordValidator implements OPasswordValidator {
  private boolean enabled = true;
  private boolean ignoreUUID = true;
  private int minLength = 0;
  private Pattern hasNumber;
  private Pattern hasSpecial;
  private Pattern hasUppercase;
  private Pattern isUUID =
      Pattern.compile(
          "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

  // OSecurityComponent
  public void active() {
    OLogManager.instance().info(this, "ODefaultPasswordValidator is active");
  }

  // OSecurityComponent
  public void config(final ODocument jsonConfig, OSecuritySystem security) {
    try {
      if (jsonConfig.containsField("enabled")) {
        enabled = jsonConfig.field("enabled");
      }

      if (jsonConfig.containsField("ignoreUUID")) {
        ignoreUUID = jsonConfig.field("ignoreUUID");
      }

      if (jsonConfig.containsField("minimumLength")) {
        minLength = jsonConfig.field("minimumLength");
      }

      if (jsonConfig.containsField("numberRegEx")) {
        hasNumber = Pattern.compile((String) jsonConfig.field("numberRegEx"));
      }

      if (jsonConfig.containsField("specialRegEx")) {
        hasSpecial = Pattern.compile((String) jsonConfig.field("specialRegEx"));
      }

      if (jsonConfig.containsField("uppercaseRegEx")) {
        hasUppercase = Pattern.compile((String) jsonConfig.field("uppercaseRegEx"));
      }
    } catch (Exception ex) {
      OLogManager.instance().error(this, "ODefaultPasswordValidator.config()", ex);
    }
  }

  // OSecurityComponent
  public void dispose() {}

  // OSecurityComponent
  public boolean isEnabled() {
    return enabled;
  }

  // OPasswordValidator
  public void validatePassword(final String username, final String password)
      throws OInvalidPasswordException {
    if (!enabled) return;

    if (password != null && !password.isEmpty()) {
      if (ignoreUUID && isUUID(password)) return;

      if (password.length() < minLength) {
        OLogManager.instance()
            .debug(
                this,
                "ODefaultPasswordValidator.validatePassword() Password length (%d) is too short",
                password.length());
        throw new OInvalidPasswordException(
            "Password length is too short.  Minimum password length is " + minLength);
      }

      if (hasNumber != null && !isValid(hasNumber, password)) {
        OLogManager.instance()
            .debug(
                this,
                "ODefaultPasswordValidator.validatePassword() Password requires a minimum count of numbers");
        throw new OInvalidPasswordException("Password requires a minimum count of numbers");
      }

      if (hasSpecial != null && !isValid(hasSpecial, password)) {
        OLogManager.instance()
            .debug(
                this,
                "ODefaultPasswordValidator.validatePassword() Password requires a minimum count of special characters");
        throw new OInvalidPasswordException(
            "Password requires a minimum count of special characters");
      }

      if (hasUppercase != null && !isValid(hasUppercase, password)) {
        OLogManager.instance()
            .debug(
                this,
                "ODefaultPasswordValidator.validatePassword() Password requires a minimum count of uppercase characters");
        throw new OInvalidPasswordException(
            "Password requires a minimum count of uppercase characters");
      }
    } else {
      OLogManager.instance()
          .debug(this, "ODefaultPasswordValidator.validatePassword() Password is null or empty");
      throw new OInvalidPasswordException(
          "ODefaultPasswordValidator.validatePassword() Password is null or empty");
    }
  }

  private boolean isValid(final Pattern pattern, final String password) {
    return pattern.matcher(password).find();
  }

  private boolean isUUID(final String password) {
    return isUUID.matcher(password).find();
  }
}

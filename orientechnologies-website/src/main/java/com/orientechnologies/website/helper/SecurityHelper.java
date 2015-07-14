package com.orientechnologies.website.helper;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.security.DeveloperAuthentication;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

/**
 * Created by Enrico Risa on 11/11/14.
 */

public class SecurityHelper {

  public static OUser currentUser() {

    Authentication auth = SecurityContextHolder.getContext().getAuthentication();

    if (auth != null && auth.isAuthenticated()) {
      DeveloperAuthentication developerAuthentication = (DeveloperAuthentication) auth;
      return developerAuthentication.getUser();
    } else {
      OLogManager.instance().warn(auth, "Not Logged");
      return null;

    }
  }

  public static String currentToken() {
    Authentication auth = SecurityContextHolder.getContext().getAuthentication();

    if (auth != null && auth.isAuthenticated()) {
      DeveloperAuthentication developerAuthentication = (DeveloperAuthentication) auth;
      return developerAuthentication.getGithubToken();
    } else {
      OLogManager.instance().warn(auth, "Not Logged");
      return null;
    }

  }

}

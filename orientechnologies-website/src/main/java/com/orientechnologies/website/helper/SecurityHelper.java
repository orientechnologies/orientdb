package com.orientechnologies.website.helper;

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
    DeveloperAuthentication developerAuthentication = (DeveloperAuthentication) auth;
    return developerAuthentication.getUser();
  }


  public static String currentToken() {
    Authentication auth = SecurityContextHolder.getContext().getAuthentication();
    DeveloperAuthentication developerAuthentication = (DeveloperAuthentication) auth;
    return developerAuthentication.getGithubToken();
  }

}

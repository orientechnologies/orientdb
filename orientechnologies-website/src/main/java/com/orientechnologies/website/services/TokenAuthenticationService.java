package com.orientechnologies.website.services;

import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;

/**
 * Created by Enrico Risa on 21/10/14.
 */

public interface TokenAuthenticationService {

  public Authentication getAuthentication(HttpServletRequest request);
}

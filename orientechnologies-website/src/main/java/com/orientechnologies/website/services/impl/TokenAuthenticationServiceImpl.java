package com.orientechnologies.website.services.impl;

import com.orientechnologies.website.model.schema.dto.Developer;
import com.orientechnologies.website.repository.DeveloperRepository;
import com.orientechnologies.website.security.DeveloperAuthentication;
import com.orientechnologies.website.services.TokenAuthenticationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;

/**
 * Created by Enrico Risa on 21/10/14.
 */
@Service
public class TokenAuthenticationServiceImpl implements TokenAuthenticationService {

  private static final String AUTH_HEADER_NAME = "X-AUTH-TOKEN";

  @Autowired
  private DeveloperRepository developerRepository;

  @Override
  public Authentication getAuthentication(HttpServletRequest request) {
    final String token = request.getHeader(AUTH_HEADER_NAME);
    if (token != null) {
      final Developer user = developerRepository.findByGithubToken(token);
      if (user != null) {
        return new DeveloperAuthentication(user);
      }
    }
    return null;
  }
}

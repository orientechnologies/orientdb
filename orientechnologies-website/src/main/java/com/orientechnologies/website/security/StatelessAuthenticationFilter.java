package com.orientechnologies.website.security;

import com.orientechnologies.website.services.TokenAuthenticationService;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.GenericFilterBean;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import java.io.IOException;

/**
 * Created by Enrico Risa on 21/10/14.
 */
public class StatelessAuthenticationFilter extends GenericFilterBean {

  private final TokenAuthenticationService tokenAuthenticationService;

  public StatelessAuthenticationFilter(TokenAuthenticationService tokenAuthenticationService) {
    this.tokenAuthenticationService = tokenAuthenticationService;
  }

  @Override
  public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException,
      ServletException {

    SecurityContextHolder.getContext().setAuthentication(
        tokenAuthenticationService.getAuthentication((javax.servlet.http.HttpServletRequest) servletRequest));
    filterChain.doFilter(servletRequest, servletResponse);
  }
}

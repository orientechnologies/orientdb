package com.orientechnologies.website.services;

import com.orientechnologies.website.exception.ServiceException;

/**
 * Created by Enrico Risa on 20/10/14.
 */
public interface DeveloperService {
  public void initUser(String token) throws ServiceException;
}

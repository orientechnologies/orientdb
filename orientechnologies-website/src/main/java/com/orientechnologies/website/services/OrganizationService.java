package com.orientechnologies.website.services;

import com.orientechnologies.website.exception.ServiceException;

public interface OrganizationService {

  public void addMember(String org, String username) throws ServiceException;
}

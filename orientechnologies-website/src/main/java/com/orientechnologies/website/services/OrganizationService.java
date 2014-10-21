package com.orientechnologies.website.services;

import com.orientechnologies.website.exception.ServiceException;
import com.orientechnologies.website.model.schema.dto.Organization;
import com.orientechnologies.website.model.schema.dto.Repository;

public interface OrganizationService {

  public void addMember(String org, String username) throws ServiceException;

  public void registerOrganization(String name) throws ServiceException;

  public Repository registerRepository(String org, String repo);

  public Organization createOrganization(String name, String description);
}

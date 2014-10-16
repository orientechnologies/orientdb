package com.orientechnologies.website.repository;

import com.orientechnologies.website.model.schema.dto.Organization;

public interface OrganizationRepository {

  public Organization findOneByName(String name);
}

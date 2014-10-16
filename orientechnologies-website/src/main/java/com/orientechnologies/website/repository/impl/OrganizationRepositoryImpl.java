package com.orientechnologies.website.repository.impl;

import com.orientechnologies.website.model.schema.dto.Organization;
import com.orientechnologies.website.repository.OrganizationRepository;
import org.springframework.stereotype.Repository;

@Repository
public class OrganizationRepositoryImpl implements OrganizationRepository {
  @Override
  public Organization findOneByName(String name) {
    return null;
  }
}

package com.orientechnologies.website.services.impl;

import com.orientechnologies.website.exception.ServiceException;
import com.orientechnologies.website.model.schema.dto.Organization;
import com.orientechnologies.website.repository.OrganizationRepository;
import com.orientechnologies.website.services.OrganizationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

/**
 * Created by Enrico Risa on 17/10/14.
 */
@Service
public class OrganizationServiceImpl implements OrganizationService {

  @Autowired
  private OrganizationRepository organizationRepository;

  @Override
  public void addMember(String org, String username) throws ServiceException {

    Organization organization = organizationRepository.findOneByName(org);
    if (organization == null) {
      throw ServiceException.create(HttpStatus.NOT_FOUND.value()).withMessage("Organization not Found");
    }

  }
}

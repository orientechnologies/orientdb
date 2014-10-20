package com.orientechnologies.website.services.impl;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.model.schema.OSiteSchema;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.orientechnologies.website.exception.ServiceException;
import com.orientechnologies.website.model.schema.dto.Developer;
import com.orientechnologies.website.model.schema.dto.Organization;
import com.orientechnologies.website.repository.DeveloperRepository;
import com.orientechnologies.website.repository.OrganizationRepository;
import com.orientechnologies.website.services.OrganizationService;

/**
 * Created by Enrico Risa on 17/10/14.
 */
@Service
public class OrganizationServiceImpl implements OrganizationService {

  @Autowired
  private OrientDBFactory        dbFactory;

  @Autowired
  private OrganizationRepository organizationRepository;

  @Autowired
  private DeveloperRepository    developerRepository;

  @Override
  public void addMember(String org, String username) throws ServiceException {

    Organization organization = organizationRepository.findOneByName(org);
    if (organization != null) {
      Developer developer = developerRepository.findUserByLogin(username);

      if (developer != null)
        createMembership(organization, developer);
      // throw ServiceException.create(HttpStatus.NOT_FOUND.value()).withMessage("Organization not Found");
    }

  }

  private void createMembership(Organization organization, Developer developer) {
    OrientGraph graph = dbFactory.getGraph();

    OrientVertex orgVertex = new OrientVertex(graph, new ORecordId(organization.getId()));
    OrientVertex devVertex = new OrientVertex(graph, new ORecordId(developer.getId()));

    orgVertex.addEdge(OSiteSchema.HasMember.class.getSimpleName(), devVertex);

    graph.commit();

  }
}

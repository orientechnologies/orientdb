package com.orientechnologies.website.services;

import com.orientechnologies.website.exception.ServiceException;
import com.orientechnologies.website.model.schema.dto.*;
import com.orientechnologies.website.model.schema.dto.web.hateoas.ScopeDTO;
import org.springframework.transaction.annotation.Transactional;

public interface OrganizationService {

    public void addMember(String org, String username) throws ServiceException;

    public void registerOrganization(String name) throws ServiceException;

    public Client registerClient(String org, Client client);

    @Transactional
    public OUser addMemberClient(String org, Integer clientId, String username);

    public Repository registerRepository(String org, String repo);

    public Organization createOrganization(String name, String description);

    @Transactional
    public Scope registerScope(String name, ScopeDTO scope, Integer id);

//  @Transactional
//  Environment registerClientEnvironment(String name, Integer id, Environment environment);

//  @Transactional
//  Sla registerClientSlaToEnvironment(String name, Integer id, String env, Sla sla);

}

package com.orientechnologies.website.services;

import com.orientechnologies.website.exception.ServiceException;
import com.orientechnologies.website.model.schema.dto.*;
import com.orientechnologies.website.model.schema.dto.web.ImportDTO;
import com.orientechnologies.website.model.schema.dto.web.hateoas.ScopeDTO;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public interface OrganizationService {

  public void addMember(String org, String username) throws ServiceException;

  public void registerOrganization(String name) throws ServiceException;

  public Client registerClient(String org, Client client);

  @Transactional
  public OUser addMemberClient(String org, Integer clientId, String username);

  public Repository registerRepository(String org, String repo, ImportDTO importRules);

  public Organization createOrganization(String name, String description);

  @Transactional
  public Scope registerScope(String name, ScopeDTO scope, Integer id);

  public void registerRoom(String name, Integer clientId);

  @Transactional
  public Message registerMessage(String name, Integer clientId, Message message);

  public List<Message> getClientRoomMessage(String name, Integer clientId, String beforeUuid);

  public List<OUser> getClientRoomActors(String name, Integer clientId);

  @Transactional
  void checkInRoom(String name, Integer clientId);

  @Transactional
  Message patchMessage(String name, Integer clientId, String messageId, Message message);

  @Transactional
  OUser registerBot(String name, String username);


  @Transactional
  Contract registerContract(String name, Contract contract);

  public void createMembership(Organization test, OUser user);
}

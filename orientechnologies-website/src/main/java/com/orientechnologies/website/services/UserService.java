package com.orientechnologies.website.services;

import com.orientechnologies.website.model.schema.dto.UserRegistration;
import com.orientechnologies.website.exception.ServiceException;
import com.orientechnologies.website.model.schema.dto.*;
import com.orientechnologies.website.model.schema.dto.web.UserDTO;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Created by Enrico Risa on 20/10/14.
 */
public interface UserService {
  public void initUser(String token) throws ServiceException;

  public UserDTO forWeb(OUser user);

  boolean isMember(OUser user, String orgName);

  boolean isTeamMember(OUser user, Repository repo);

  public Client getClient(OUser user, String orgName);

  public boolean isSupport(OUser user, String orgName);

  public boolean isClient(OUser user, String orgName);

  @Transactional
  public Environment registerUserEnvironment(OUser user, Environment environment);

  public void deregisterUserEnvironment(OUser user, Long environmentId);

  public Environment patchUserEnvironment(OUser user, Long environmentId, Environment environment);

  public List<Environment> getUserEnvironments(OUser user);

  public OUser patchUser(OUser current, UserDTO user);

  void registerUser(UserRegistration user);

  UserToken login(UserRegistration user);

  public void profileIssue(OUser current, Issue issue, String organization);

  public void profileEvent(OUser user, Event event, String organization);

  public void profileUser(OUser current, OUser toProfile, String organization);

  void resetPassword(UserRegistration user);
}

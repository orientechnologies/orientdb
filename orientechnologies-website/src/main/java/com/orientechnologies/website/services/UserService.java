package com.orientechnologies.website.services;

import com.orientechnologies.website.exception.ServiceException;
import com.orientechnologies.website.model.schema.dto.Client;
import com.orientechnologies.website.model.schema.dto.Environment;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.model.schema.dto.Repository;
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

  @Transactional
  public Environment registerUserEnvironment(OUser user, Environment environment);

  public void deregisterUserEnvironment(OUser user, Long environmentId);

  public Environment patchUserEnvironment(OUser user, Long environmentId, Environment environment);

  public List<Environment> getUserEnvironments(OUser user);
}

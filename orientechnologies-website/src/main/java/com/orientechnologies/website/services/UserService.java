package com.orientechnologies.website.services;

import com.orientechnologies.website.exception.ServiceException;
import com.orientechnologies.website.model.schema.dto.Client;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.model.schema.dto.web.UserDTO;

/**
 * Created by Enrico Risa on 20/10/14.
 */
public interface UserService {
  public void initUser(String token) throws ServiceException;

  public UserDTO forWeb(OUser user);

  boolean isMember(OUser user, String orgName);

  Client getClient(OUser user, String orgName);
}

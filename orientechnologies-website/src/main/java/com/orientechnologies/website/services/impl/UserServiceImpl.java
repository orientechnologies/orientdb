package com.orientechnologies.website.services.impl;

import com.orientechnologies.website.github.GUser;
import com.orientechnologies.website.github.GitHub;
import com.orientechnologies.website.model.schema.dto.Client;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.model.schema.dto.Organization;
import com.orientechnologies.website.model.schema.dto.Repository;
import com.orientechnologies.website.model.schema.dto.web.UserDTO;
import com.orientechnologies.website.repository.UserRepository;
import com.orientechnologies.website.services.OrganizationService;
import com.orientechnologies.website.services.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Created by Enrico Risa on 20/10/14.
 */
@Service
public class UserServiceImpl implements UserService {

  @Autowired
  private UserRepository      userRepository;

  @Autowired
  private OrganizationService organizationService;

  @Transactional
  @Override
  public void initUser(String token) {

    try {
      GitHub github = new GitHub(token);
      GUser self = github.user();
      OUser user = userRepository.findUserByLogin(self.getLogin());
      String email = self.getEmail();

      if (user == null) {
        user = new OUser(self.getLogin(), token, email);
        user.setId(self.getId());
      } else {
        user.setToken(token);
        user.setEmail(email);
      }
      userRepository.save(user);

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public UserDTO forWeb(OUser user) {
    UserDTO userDTO = new UserDTO();
    userDTO.setId(user.getId());
    userDTO.setName(user.getName());
    List<Repository> repositoryList = userRepository.findMyRepositories(user.getUsername());
    userDTO.setClientsOf(userRepository.findMyClientOrganization(user.getUsername()));
    userDTO.setClients(userRepository.findAllMyClientMember(user.getUsername()));
    userDTO.setRepositories(repositoryList);
    return userDTO;
  }

  @Override
  public boolean isMember(OUser user, String orgName) {

    List<Organization> myorganization = userRepository.findMyorganization(user.getUsername());
    for (Organization organization : myorganization) {
      if (organization.getName().equals(orgName)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isTeamMember(OUser user, Repository repo) {
    return isMember(user, repo.getOrganization().getName());
  }

  @Override
  public Client getClient(OUser user, String orgName) {
    return userRepository.findMyClientMember(user.getUsername(), orgName);
  }
}

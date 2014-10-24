package com.orientechnologies.website.services.impl;

import com.orientechnologies.website.model.schema.dto.User;
import com.orientechnologies.website.services.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.jcabi.github.Github;
import com.jcabi.github.RtGithub;
import com.orientechnologies.website.repository.UserRepository;
import com.orientechnologies.website.services.OrganizationService;

/**
 * Created by Enrico Risa on 20/10/14.
 */
@Service
public class UserServiceImpl implements UserService {

  @Autowired
  private UserRepository userRepository;

  @Autowired
  private OrganizationService organizationService;

  @Override
  public void initUser(String token) {

    try {
      Github github = new RtGithub(token);
      com.jcabi.github.User self = github.users().self();
      User user = userRepository.findUserByLogin(self.login());
      String email = self.emails().iterate().iterator().next();
      if (user == null) {
        user = new User(self.login(), token, email);
      } else {
        user.setToken(token);
        user.setEmail(email);
      }
      userRepository.save(user);

      // do not subscribe directly to all organizations
      // for (Organization organization : self.organizations().iterate()) {
      // organizationService.addMember(organization.login(), self.login());
      // }

    } catch (Exception e) {

    }
  }
}

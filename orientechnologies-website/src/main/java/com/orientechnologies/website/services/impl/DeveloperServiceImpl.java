package com.orientechnologies.website.services.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.jcabi.github.Github;
import com.jcabi.github.Organization;
import com.jcabi.github.RtGithub;
import com.jcabi.github.User;
import com.orientechnologies.website.model.schema.dto.Developer;
import com.orientechnologies.website.repository.DeveloperRepository;
import com.orientechnologies.website.services.DeveloperService;
import com.orientechnologies.website.services.OrganizationService;

/**
 * Created by Enrico Risa on 20/10/14.
 */
@Service
public class DeveloperServiceImpl implements DeveloperService {

  @Autowired
  private DeveloperRepository developerRepository;

  @Autowired
  private OrganizationService organizationService;

  @Override
  public void initUser(String token) {

    try {
      Github github = new RtGithub(token);
      User self = github.users().self();
      Developer developer = developerRepository.findUserByLogin(self.login());
      String email = self.emails().iterate().iterator().next();
      if (developer == null) {
        developer = new Developer(self.login(), token, email);
      } else {
        developer.setToken(token);
        developer.setEmail(email);
      }
      developerRepository.save(developer);

      // do not subscribe directly to all organizations
      // for (Organization organization : self.organizations().iterate()) {
      // organizationService.addMember(organization.login(), self.login());
      // }

    } catch (Exception e) {

    }
  }
}

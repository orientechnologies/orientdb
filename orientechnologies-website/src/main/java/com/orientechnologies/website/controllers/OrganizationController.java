package com.orientechnologies.website.controllers;

import com.orientechnologies.website.services.OrganizationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.orientechnologies.website.model.schema.dto.Organization;
import com.orientechnologies.website.repository.OrganizationRepository;

@RestController
@EnableAutoConfiguration
public class OrganizationController {

  @Autowired
  private OrganizationRepository orgRepository;

  @Autowired
  private OrganizationService    organizationService;

  @RequestMapping(value = "/org/{name}", method = RequestMethod.GET)
  public ResponseEntity<Organization> getOrganizationInfo(@PathVariable("name") String name) {

    Organization organization = orgRepository.findOneByName(name);
    if (organization == null) {
      return new ResponseEntity<Organization>(HttpStatus.NOT_FOUND);
    } else {
      return new ResponseEntity<Organization>(organization, HttpStatus.OK);
    }
  }

  @RequestMapping(value = "/org/{name}/members/{username}", method = RequestMethod.PUT)
  @ResponseStatus(HttpStatus.OK)
  public void addMemberToOrg(@PathVariable("name") String name, @PathVariable("name") String username) {

    organizationService.addMember(name, username);
  }
}

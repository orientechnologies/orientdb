package com.orientechnologies.website.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.orientechnologies.website.model.schema.dto.Organization;
import com.orientechnologies.website.repository.OrganizationRepository;

@RestController
@EnableAutoConfiguration
public class OrganizationController {

  @Autowired
  private OrganizationRepository orgRepository;

  @RequestMapping(value = "/organization/{name}", method = RequestMethod.GET)
  public ResponseEntity<Organization> getOrganizationInfo(@PathVariable("name") String name) {

    Organization organization = orgRepository.findOneByName(name);
    if (organization == null) {
      return new ResponseEntity<Organization>(HttpStatus.NOT_FOUND);
    } else {
      return new ResponseEntity<Organization>(organization, HttpStatus.OK);
    }
  }
}

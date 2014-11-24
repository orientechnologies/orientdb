package com.orientechnologies.website.controllers;

import com.orientechnologies.website.configuration.ApiVersion;
import com.orientechnologies.website.model.schema.dto.*;
import com.orientechnologies.website.model.schema.dto.web.IssueDTO;
import com.orientechnologies.website.repository.OrganizationRepository;
import com.orientechnologies.website.repository.RepositoryRepository;
import com.orientechnologies.website.services.OrganizationService;
import com.orientechnologies.website.services.RepositoryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@EnableAutoConfiguration
@RequestMapping("orgs")
@ApiVersion(1)
public class OrganizationController {

  @Autowired
  private OrganizationRepository orgRepository;

  @Autowired
  private OrganizationService    organizationService;

  @Autowired
  private RepositoryRepository   repoRepository;

  @Autowired
  private RepositoryService      repositoryService;

  @RequestMapping(value = "{name}", method = RequestMethod.GET)
  public ResponseEntity<Organization> getOrganizationInfo(@PathVariable("name") String name) {

    Organization organization = orgRepository.findOneByName(name);
    if (organization == null) {
      return new ResponseEntity<Organization>(HttpStatus.NOT_FOUND);
    } else {
      return new ResponseEntity<Organization>(organization, HttpStatus.OK);
    }
  }

  @RequestMapping(value = "{name}/issues", method = RequestMethod.GET)
  public ResponseEntity<List<Issue>> getOrganizationIssues(@PathVariable("name") String name, @RequestParam(value = "q") String q) {
    return new ResponseEntity<List<Issue>>(orgRepository.findOrganizationIssues(name, q), HttpStatus.OK);
  }

  @RequestMapping(value = "{name}/repos", method = RequestMethod.GET)
  public ResponseEntity<List<Repository>> getOrganizationRepositories(@PathVariable("name") String name) {
    return new ResponseEntity<List<Repository>>(orgRepository.findOrganizationRepositories(name), HttpStatus.OK);
  }

  @RequestMapping(value = "{name}/members/{username}", method = RequestMethod.POST)
  @ResponseStatus(HttpStatus.OK)
  public void addMemberToOrg(@PathVariable("name") String name, @PathVariable("username") String username) {

    organizationService.addMember(name, username);
  }

  @RequestMapping(value = "{name}/clients", method = RequestMethod.POST)
  @ResponseStatus(HttpStatus.OK)
  public Client addClientToOrg(@PathVariable("name") String name, @RequestBody Client client) {
    return organizationService.registerClient(name, client);
  }

  @RequestMapping(value = "{name}/clients/{id}/members/{username}", method = RequestMethod.POST)
  @ResponseStatus(HttpStatus.OK)
  public OUser addMemberClientToOrg(@PathVariable("name") String name, @PathVariable("id") Integer id,
      @PathVariable("username") String username) {
    return organizationService.addMemberClient(name, id, username);
  }

  @RequestMapping(value = "{name}/clients", method = RequestMethod.GET)
  @ResponseStatus(HttpStatus.OK)
  public List<Client> findClients(@PathVariable("name") String name) {
    return orgRepository.findClients(name);
  }

  @RequestMapping(value = "{name}/clients/{id}", method = RequestMethod.GET)
  @ResponseStatus(HttpStatus.OK)
  public Client findClient(@PathVariable("name") String name, @PathVariable("id") Integer id) {
    return orgRepository.findClient(name, id);
  }

  @RequestMapping(value = "{name}/clients/{id}/members", method = RequestMethod.GET)
  @ResponseStatus(HttpStatus.OK)
  public List<OUser> findClientMembers(@PathVariable("name") String name, @PathVariable("id") Integer id) {
    return orgRepository.findClientMembers(name, id);
  }

  @RequestMapping(value = "{name}", method = RequestMethod.POST)
  public ResponseEntity<Organization> registerOrganization(@PathVariable("name") String name) {
    Organization organization = orgRepository.findOneByName(name);

    if (organization == null) {
      organizationService.registerOrganization(name);
      return new ResponseEntity<Organization>(organization, HttpStatus.OK);
    } else {
      return new ResponseEntity<Organization>(HttpStatus.CONFLICT);
    }
  }

  @RequestMapping(value = "{name}/priorities", method = RequestMethod.GET)
  @ResponseStatus(HttpStatus.OK)
  public List<Priority> findPriorities(@PathVariable("name") String name) {
    return orgRepository.findPriorities(name);
  }

  @RequestMapping(value = "{name}/issues", method = RequestMethod.POST)
  public ResponseEntity<Issue> createIssue(@PathVariable("name") String owner, @RequestBody IssueDTO issue) {

    Repository r = orgRepository.findOrganizationRepositoryByScope(owner, issue.getScope());

    return r != null ? new ResponseEntity<Issue>(repositoryService.openIssue(r, issue), HttpStatus.OK) : new ResponseEntity<Issue>(
        HttpStatus.NOT_FOUND);
  }

  @RequestMapping(value = "{name}/scopes", method = RequestMethod.GET)
  @ResponseStatus(HttpStatus.OK)
  public List<Scope> findScopes(@PathVariable("name") String name) {
    return orgRepository.findScopes(name);
  }

  @RequestMapping(value = "{name}/repos/{repo}", method = RequestMethod.POST)
  public ResponseEntity<Repository> registerRepository(@PathVariable("name") String name, @PathVariable("repo") String repo) {

    Repository rep = organizationService.registerRepository(name, repo);
    return new ResponseEntity<Repository>(rep, HttpStatus.OK);
  }
}

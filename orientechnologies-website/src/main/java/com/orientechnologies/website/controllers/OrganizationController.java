package com.orientechnologies.website.controllers;

import com.orientechnologies.website.configuration.ApiVersion;
import com.orientechnologies.website.hateoas.Page;
import com.orientechnologies.website.hateoas.assembler.IssueAssembler;
import com.orientechnologies.website.hateoas.assembler.PagedResourceAssembler;
import com.orientechnologies.website.helper.SecurityHelper;
import com.orientechnologies.website.model.schema.dto.*;
import com.orientechnologies.website.model.schema.dto.web.ImportDTO;
import com.orientechnologies.website.model.schema.dto.web.IssueDTO;
import com.orientechnologies.website.model.schema.dto.web.hateoas.IssueResource;
import com.orientechnologies.website.model.schema.dto.web.hateoas.ScopeDTO;
import com.orientechnologies.website.repository.OrganizationRepository;
import com.orientechnologies.website.repository.RepositoryRepository;
import com.orientechnologies.website.services.OrganizationService;
import com.orientechnologies.website.services.RepositoryService;
import com.orientechnologies.website.services.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.hateoas.PagedResources;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@EnableAutoConfiguration
@RequestMapping(ApiUrls.ORGS_V1)
@ApiVersion(1)
public class OrganizationController extends ExceptionController {

  @Autowired
  private OrganizationRepository orgRepository;

  @Autowired
  private OrganizationService    organizationService;

  @Autowired
  private RepositoryRepository   repoRepository;

  @Autowired
  private RepositoryService      repositoryService;

  @Autowired
  private IssueAssembler         issueAssembler;

  @Autowired
  private PagedResourceAssembler pagedResourceAssembler;

  @Autowired
  private UserService            userService;

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
  public ResponseEntity<PagedResources<IssueResource>> getOrganizationIssuesPaged(@PathVariable("name") String name,
      @RequestParam(value = "q", defaultValue = "") String q, @RequestParam(value = "page", defaultValue = "1") String page,
      @RequestParam(value = "per_page", defaultValue = "10") String perPage) {

    Page<Issue> issues = orgRepository.findOrganizationIssuesPagedProfiled(name, q, page, perPage);
    return new ResponseEntity<PagedResources<IssueResource>>(pagedResourceAssembler.toResource(issues, issueAssembler),
        HttpStatus.OK);
  }

  @RequestMapping(value = "{name}/issues/{number}", method = RequestMethod.GET)
  public ResponseEntity<Issue> getOrganizationSingleIssue(@PathVariable("name") String organization,
      @PathVariable("number") Long number) {

    Issue issue = orgRepository.findSingleOrganizationIssueByNumber(organization, number);
    return issue != null ? new ResponseEntity<Issue>(issue, HttpStatus.OK) : new ResponseEntity<Issue>(HttpStatus.NOT_FOUND);

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

  @RequestMapping(value = "{name}/members", method = RequestMethod.GET)
  @ResponseStatus(HttpStatus.OK)
  public List<OUser> findMembers(@PathVariable("name") String name) {
    return orgRepository.findMembers(name);
  }

  @RequestMapping(value = "{name}/milestones", method = RequestMethod.GET)
  @ResponseStatus(HttpStatus.OK)
  public List<Milestone> findMilestones(@PathVariable("name") String name) {
    return orgRepository.findMilestones(name);
  }

  @RequestMapping(value = "{name}/labels", method = RequestMethod.GET)
  @ResponseStatus(HttpStatus.OK)
  public List<Label> findLabels(@PathVariable("name") String name) {
    return orgRepository.findLabels(name);
  }

  @RequestMapping(value = "{name}/clients", method = RequestMethod.POST)
  @ResponseStatus(HttpStatus.OK)
  public Client addClientToOrg(@PathVariable("name") String name, @RequestBody Client client) {
    return organizationService.registerClient(name, client);
  }

  @RequestMapping(value = "{name}/clients/{id}/room", method = RequestMethod.POST)
  @ResponseStatus(HttpStatus.OK)
  public ResponseEntity registerRoom(@PathVariable("name") String name, @PathVariable("id") Integer clientId) {
    organizationService.registerRoom(name, clientId);
    return new ResponseEntity(HttpStatus.OK);
  }

  @RequestMapping(value = "{name}/clients/{id}/room", method = RequestMethod.GET)
  @ResponseStatus(HttpStatus.OK)
  public ResponseEntity<List<Message>> getRoomMessages(@PathVariable("name") String name, @PathVariable("id") Integer clientId,
      @RequestParam(value = "before", defaultValue = "") String beforeUuid) {
    return new ResponseEntity(organizationService.getClientRoomMessage(name, clientId, beforeUuid), HttpStatus.OK);
  }

  @RequestMapping(value = "{name}/clients/{id}/room/actors", method = RequestMethod.GET)
  @ResponseStatus(HttpStatus.OK)
  public ResponseEntity<List<OUser>> getRoomActors(@PathVariable("name") String name, @PathVariable("id") Integer clientId,
      @RequestParam(value = "before", defaultValue = "") String beforeUuid) {
    return new ResponseEntity(organizationService.getClientRoomActors(name, clientId), HttpStatus.OK);
  }

  @RequestMapping(value = "{name}/clients/{id}/room", method = RequestMethod.PATCH)
  @ResponseStatus(HttpStatus.OK)
  public ResponseEntity<Message> registerMessage(@PathVariable("name") String name, @PathVariable("id") Integer clientId,
      @RequestBody Message message) {
    return new ResponseEntity(organizationService.registerMessage(name, clientId, message), HttpStatus.OK);
  }

  @RequestMapping(value = "{name}/clients/{id}/members/{username}", method = RequestMethod.POST)
  @ResponseStatus(HttpStatus.OK)
  public OUser addMemberClientToOrg(@PathVariable("name") String name, @PathVariable("id") Integer id,
      @PathVariable("username") String username) {
    return organizationService.addMemberClient(name, id, username);
  }

  @RequestMapping(value = "{name}/clients", method = RequestMethod.GET)
  @ResponseStatus(HttpStatus.OK)
  public ResponseEntity<List<Client>> findClients(@PathVariable("name") String name) {

    OUser user = SecurityHelper.currentUser();
    if (userService.isMember(user, name)) {
      return new ResponseEntity<List<Client>>(orgRepository.findClients(name), HttpStatus.OK);
    } else {
      return new ResponseEntity<List<Client>>(HttpStatus.NOT_FOUND);
    }
  }

  @RequestMapping(value = "{name}/rooms", method = RequestMethod.GET)
  @ResponseStatus(HttpStatus.OK)
  public ResponseEntity<List<Client>> findRooms(@PathVariable("name") String name) {

    OUser user = SecurityHelper.currentUser();
    if (userService.isMember(user, name)) {
      return new ResponseEntity<List<Client>>(orgRepository.findClients(name), HttpStatus.OK);
    } else {
      return new ResponseEntity<List<Client>>(HttpStatus.NOT_FOUND);
    }
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

  @RequestMapping(value = "{name}/clients/{id}/environments", method = RequestMethod.GET)
  @ResponseStatus(HttpStatus.OK)
  public List<Environment> findClientEnvironments(@PathVariable("name") String name, @PathVariable("id") Integer id) {
    return orgRepository.findClientEnvironments(name, id);
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
  public ResponseEntity<Repository> registerRepository(@PathVariable("name") String name, @PathVariable("repo") String repo,
      @RequestBody ImportDTO importRules) {

    Repository rep = organizationService.registerRepository(name, repo, importRules);
    return new ResponseEntity<Repository>(rep, HttpStatus.OK);
  }

  @RequestMapping(value = "{name}/scopes", method = RequestMethod.POST)
  @ResponseStatus(HttpStatus.OK)
  public Scope registerScope(@PathVariable("name") String name, @RequestBody ScopeDTO scope) {
    return organizationService.registerScope(name, scope, null);
  }

  @RequestMapping(value = "{name}/scopes/{id}", method = RequestMethod.PATCH)
  @ResponseStatus(HttpStatus.OK)
  public Scope patchScope(@PathVariable("name") String name, @PathVariable("id") Integer id, @RequestBody ScopeDTO scope) {
    return organizationService.registerScope(name, scope, id);
  }

}

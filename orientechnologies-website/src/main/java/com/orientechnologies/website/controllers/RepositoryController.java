package com.orientechnologies.website.controllers;

import com.orientechnologies.website.configuration.ApiVersion;
import com.orientechnologies.website.model.schema.dto.*;
import com.orientechnologies.website.repository.OrganizationRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@EnableAutoConfiguration
@RequestMapping("repos")
@ApiVersion(1)
public class RepositoryController {

  @Autowired
  private OrganizationRepository organizationRepository;

  @RequestMapping(value = "{owner}/{repo}/issues/{number}", method = RequestMethod.GET)
  public ResponseEntity<Issue> getSingleIssue(@PathVariable("owner") String owner, @PathVariable("repo") String repo,
      @PathVariable("number") String number) {

    return new ResponseEntity<Issue>(organizationRepository.findSingleOrganizationIssueByRepoAndNumber(owner, repo, number),
        HttpStatus.OK);
  }

  @RequestMapping(value = "{owner}/{repo}/issues/{number}/comments", method = RequestMethod.GET)
  public ResponseEntity<List<Comment>> getSingleIssueComments(@PathVariable("owner") String owner,
      @PathVariable("repo") String repo, @PathVariable("number") String number) {
    return new ResponseEntity<List<Comment>>(organizationRepository.findSingleOrganizationIssueCommentByRepoAndNumber(owner, repo,
        number), HttpStatus.OK);
  }

  @RequestMapping(value = "{owner}/{repo}/issues/{number}/events", method = RequestMethod.GET)
  public ResponseEntity<List<Event>> getSingleIssueEvents(@PathVariable("owner") String owner, @PathVariable("repo") String repo,
      @PathVariable("number") String number) {
    return new ResponseEntity<List<Event>>(organizationRepository.findEventsByOwnerRepoAndIssueNumber(owner, repo, number),
        HttpStatus.OK);
  }

  @RequestMapping(value = "{owner}/{repo}/teams", method = RequestMethod.GET)
  public ResponseEntity<List<User>> getRepositoryTeams(@PathVariable("owner") String owner, @PathVariable("repo") String repo) {
    return new ResponseEntity<List<User>>(organizationRepository.findTeamMembers(owner, repo), HttpStatus.OK);
  }

  @RequestMapping(value = "{owner}/{repo}/milestones", method = RequestMethod.GET)
  public ResponseEntity<List<Milestone>> getRepositoryMilestones(@PathVariable("owner") String owner,
      @PathVariable("repo") String repo) {
    return new ResponseEntity<List<Milestone>>(organizationRepository.findRepoMilestones(owner, repo), HttpStatus.OK);
  }
}

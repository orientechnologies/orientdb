package com.orientechnologies.website.controllers;

import com.orientechnologies.website.configuration.ApiVersion;
import com.orientechnologies.website.exception.ServiceException;
import com.orientechnologies.website.helper.SecurityHelper;
import com.orientechnologies.website.model.schema.dto.*;
import com.orientechnologies.website.model.schema.dto.web.IssueDTO;
import com.orientechnologies.website.repository.AttachmentRepository;
import com.orientechnologies.website.repository.OrganizationRepository;
import com.orientechnologies.website.repository.RepositoryRepository;
import com.orientechnologies.website.security.OSecurityManager;
import com.orientechnologies.website.security.Permissions;
import com.orientechnologies.website.services.IssueService;
import com.orientechnologies.website.services.OrganizationService;
import com.orientechnologies.website.services.RepositoryService;
import com.orientechnologies.website.services.UserService;
import org.apache.commons.vfs2.FileObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.List;

@RestController
@EnableAutoConfiguration
@RequestMapping(ApiUrls.REPOS_V1)
@ApiVersion(1)
public class RepositoryController {

  @Autowired
  private OrganizationRepository organizationRepository;

  @Autowired
  private OrganizationService    organizationService;

  @Autowired
  private RepositoryService      repositoryService;

  @Autowired
  private RepositoryRepository   repoRepository;

  @Autowired
  UserService                    userService;
  @Autowired
  protected IssueService         issueService;

  @Autowired
  protected AttachmentRepository attachmentRepository;

  @Autowired
  protected OSecurityManager     securityManager;

//  @RequestMapping(value = "{owner}/{repo}/issues/{number}", method = RequestMethod.GET)
  public ResponseEntity<Issue> getSingleIssue(@PathVariable("owner") String owner, @PathVariable("repo") String repo,
      @PathVariable("number") Long number) {

    Issue issue = organizationRepository.findSingleOrganizationIssueByRepoAndNumber(owner, repo, number);

    return issue != null ? new ResponseEntity<Issue>(issue, HttpStatus.OK) : new ResponseEntity<Issue>(HttpStatus.NOT_FOUND);
  }

  @RequestMapping(value = "{owner}/{repo}/issues/{number}/sync", method = RequestMethod.POST)
  public ResponseEntity<Issue> syncIssue(@PathVariable("owner") String owner, @PathVariable("repo") String repo,
      @PathVariable("number") Long number) {

    Issue issue = organizationRepository.findSingleOrganizationIssueByRepoAndNumber(owner, repo, number);

    return issue != null ? new ResponseEntity<Issue>(issueService.synchIssue(issue, null), HttpStatus.OK)
        : new ResponseEntity<Issue>(HttpStatus.NOT_FOUND);
  }

  @RequestMapping(value = "{owner}/{repo}/sync", method = RequestMethod.POST)
  public ResponseEntity synchRepo(@PathVariable("owner") String owner, @PathVariable("repo") String repo) {

    Repository r = organizationRepository.findOrganizationRepository(owner, repo);

    if (r != null) {

      if (userService.isMember(SecurityHelper.currentUser(), owner)) {
        repositoryService.syncRepository(r);
        return new ResponseEntity(HttpStatus.OK);
      } else {
        return new ResponseEntity(HttpStatus.UNAUTHORIZED);
      }

    } else {

      return new ResponseEntity(HttpStatus.NOT_FOUND);
    }

  }

  @RequestMapping(value = "{owner}/{repo}/issues/{number}/comments", method = RequestMethod.GET)
  public ResponseEntity<List<Comment>> getSingleIssueComments(@PathVariable("owner") String owner,
      @PathVariable("repo") String repo, @PathVariable("number") Long number) {
    return new ResponseEntity<List<Comment>>(organizationRepository.findSingleOrganizationIssueCommentByRepoAndNumber(owner, repo,
        number), HttpStatus.OK);
  }

  @RequestMapping(value = "{owner}/{repo}/issues/{number}/events", method = RequestMethod.GET)
  public ResponseEntity<List<Event>> getSingleIssueEvents(@PathVariable("owner") String owner, @PathVariable("repo") String repo,
      @PathVariable("number") Long number) {
    List<Event> events = organizationRepository.findEventsByOwnerRepoAndIssueNumber(owner, repo, number);

    for (Event event : events) {
      userService.profileEvent(SecurityHelper.currentUser(), event, owner);
    }
    return events != null ? new ResponseEntity<List<Event>>(events, HttpStatus.OK) : new ResponseEntity<List<Event>>(
        HttpStatus.NOT_FOUND);
  }

  @RequestMapping(value = "{owner}/{repo}/issues/{number}/attachments", method = RequestMethod.GET)
  public ResponseEntity<List<Attachment>> getSingleIssueAttachments(@PathVariable("owner") String owner,
      @PathVariable("repo") String repo, @PathVariable("number") Long number) {

    Issue issue = organizationRepository.findSingleOrganizationIssueByRepoAndNumber(owner, repo, number);

    if (Boolean.TRUE.equals(issue.getConfidential()) && issue.getClient() != null) {

      if (securityManager.isCurrentMemberOrSupport(owner) || securityManager.isCurrentClient(owner, issue.getClient())) {
        return new ResponseEntity<List<Attachment>>(attachmentRepository.findIssueAttachment(owner, issue), HttpStatus.OK);
      }

    }
    return new ResponseEntity<List<Attachment>>(HttpStatus.NOT_FOUND);
  }

  @RequestMapping(value = "{owner}/{repo}/issues/{number}/attachments", method = RequestMethod.POST)
  public ResponseEntity<Attachment> postSingleIssueAttachment(@PathVariable("owner") String owner,
      @PathVariable("repo") String repo, @PathVariable("number") Long number, @RequestParam("file") MultipartFile file) {

    Issue issue = organizationRepository.findSingleOrganizationIssueByRepoAndNumber(owner, repo, number);
    if (Boolean.TRUE.equals(issue.getConfidential() && issue.getClient() != null)) {

      if (securityManager.isCurrentMemberOrSupport(owner) || securityManager.isCurrentClient(owner, issue.getClient())) {

        try {
          Attachment attachment = attachmentRepository.attachToIssue(owner, issue, file.getOriginalFilename(),
              file.getInputStream());

          if (attachment != null) {
            return new ResponseEntity<Attachment>(attachment, HttpStatus.OK);
          } else {
            throw ServiceException.create(11, "Error uploading file:" + file.getOriginalFilename());
          }
        } catch (IOException e) {
          throw ServiceException.create(11, "Error uploading file:" + file.getOriginalFilename());
        }
      }
    }
    return new ResponseEntity(HttpStatus.NOT_FOUND);
  }

  @RequestMapping(value = "{owner}/{repo}/issues/{number}/attachments/{name:.*}", method = RequestMethod.GET)
  public ResponseEntity<InputStreamResource> getSingleIssueAttachment(@PathVariable("owner") String owner,
      @PathVariable("repo") String repo, @PathVariable("number") Long number, @PathVariable("name") String name) {

    Issue issue = organizationRepository.findSingleOrganizationIssueByRepoAndNumber(owner, repo, number);
    if (Boolean.TRUE.equals(issue.getConfidential()) && issue.getClient() != null) {
      if (securityManager.isCurrentMemberOrSupport(owner) || securityManager.isCurrentClient(owner, issue.getClient())) {
        try {

          FileObject fileObject = attachmentRepository.downloadAttachments(owner, issue, name);

          if (fileObject != null) {
            HttpHeaders respHeaders = new HttpHeaders();
            respHeaders.setContentType(MediaType.parseMediaType("application/force-download"));
            respHeaders.setContentLength(fileObject.getContent().getSize());
            respHeaders.set("Content-Disposition", "attachment; filename=" + name);
            InputStreamResource inputStreamResource = new InputStreamResource(fileObject.getContent().getInputStream());
            return new ResponseEntity<InputStreamResource>(inputStreamResource, respHeaders, HttpStatus.OK);
          }

          return new ResponseEntity(HttpStatus.NOT_FOUND);
        } catch (IOException e) {
          throw ServiceException.create(11, "Error downloading file:" + name);
        }
      }
    }
    return new ResponseEntity(HttpStatus.NOT_FOUND);
  }

  @RequestMapping(value = "{owner}/{repo}/issues/{number}/attachments/{name:.*}", method = RequestMethod.DELETE)
  public ResponseEntity deleteSingleIssueAttachment(@PathVariable("owner") String owner, @PathVariable("repo") String repo,
      @PathVariable("number") Long number, @PathVariable("name") String name) {

    Issue issue = organizationRepository.findSingleOrganizationIssueByRepoAndNumber(owner, repo, number);
    if (Boolean.TRUE.equals(issue.getConfidential())) {

      if (securityManager.isCurrentMemberOrSupport(owner) || securityManager.isCurrentClient(owner, issue.getClient())) {
        attachmentRepository.deleteAttachment(owner, issue, name);
        return new ResponseEntity(HttpStatus.OK);
      }
    }
    return new ResponseEntity(HttpStatus.NOT_FOUND);
  }

  @Deprecated
  @RequestMapping(value = "{owner}/{repo}/issues", method = RequestMethod.POST)
  public ResponseEntity<Issue> createIssue(@PathVariable("owner") String owner, @PathVariable("repo") String repo,
      @RequestBody IssueDTO issue) {

    Repository r = organizationRepository.findOrganizationRepository(owner, repo);

    // return r != null ? new ResponseEntity<Issue>(repositoryService.openIssue(r, issue), HttpStatus.OK) : new
    // ResponseEntity<Issue>(
    // HttpStatus.NOT_FOUND);
    return new ResponseEntity<Issue>(HttpStatus.NOT_FOUND);
  }

  @RequestMapping(value = "{owner}/{repo}/issues/{number}", method = RequestMethod.PATCH)
  public ResponseEntity<Issue> patchIssue(@PathVariable("owner") String owner, @PathVariable("repo") String repo,
      @PathVariable("number") Long number, @RequestBody IssueDTO issue) {

    Issue i = organizationRepository.findSingleOrganizationIssueByRepoAndNumber(owner, repo, number);
    return i != null ? new ResponseEntity<Issue>(repositoryService.patchIssue(i, null, issue), HttpStatus.OK)
        : new ResponseEntity<Issue>(HttpStatus.NOT_FOUND);
  }

  @RequestMapping(value = "{owner}/{repo}/issues/{number}/escalate", method = RequestMethod.POST)
  public ResponseEntity<Issue> escalateIssue(@PathVariable("owner") String owner, @PathVariable("repo") String repo,
      @PathVariable("number") Long number) {

    Issue i = organizationRepository.findSingleOrganizationIssueByRepoAndNumber(owner, repo, number);
    ResponseEntity responseEntity;
    if (securityManager.isCurrentClient(owner, securityManager.currentUser(), i.getClient())) {
      repositoryService.escalateIssue(i);
      responseEntity = new ResponseEntity<Issue>(i, HttpStatus.OK);
    } else {
      responseEntity = new ResponseEntity<Issue>(HttpStatus.NOT_FOUND);
    }
    return responseEntity;
  }

  @PreAuthorize(Permissions.ISSUE_LABEL)
  @RequestMapping(value = "{owner}/{repo}/issues/{number}/labels", method = RequestMethod.POST)
  public ResponseEntity<List<Label>> addLabels(@PathVariable("owner") String owner, @PathVariable("repo") String repo,
      @PathVariable("number") Long number, @RequestBody List<String> labels) {

    Issue i = organizationRepository.findSingleOrganizationIssueByRepoAndNumber(owner, repo, number);

    if (i == null) {
      return new ResponseEntity<List<Label>>(HttpStatus.NOT_FOUND);
    }

    OUser user = Boolean.TRUE.equals(i.getConfidential()) ? null : securityManager.botIfSupport(owner);

    return new ResponseEntity<List<Label>>(
        issueService.addLabels(i, labels, user, true, !Boolean.TRUE.equals(i.getConfidential())), HttpStatus.OK);
  }

  @PreAuthorize(Permissions.ISSUE_LABEL)
  @RequestMapping(value = "{owner}/{repo}/issues/{number}/labels/{lname}", method = RequestMethod.DELETE)
  public ResponseEntity<List<Label>> deleteLabel(@PathVariable("owner") String owner, @PathVariable("repo") String repo,
      @PathVariable("number") Long number, @PathVariable("lname") String lname) {

    Issue i = organizationRepository.findSingleOrganizationIssueByRepoAndNumber(owner, repo, number);
    if (i == null) {
      return new ResponseEntity<List<Label>>(HttpStatus.NOT_FOUND);
    }
    OUser user = Boolean.TRUE.equals(i.getConfidential()) ? null : securityManager.botIfSupport(owner);
    issueService.removeLabel(i, lname, user, !Boolean.TRUE.equals(i.getConfidential()));
    return new ResponseEntity<List<Label>>(HttpStatus.OK);
  }

  @RequestMapping(value = "{owner}/{repo}/issues/{number}/comments", method = RequestMethod.POST)
  public ResponseEntity<Comment> postComment(@PathVariable("owner") String owner, @PathVariable("repo") String repo,
      @PathVariable("number") Long number, @RequestBody Comment comment) {

    Issue i = organizationRepository.findSingleOrganizationIssueByRepoAndNumber(owner, repo, number);

    return i != null ? new ResponseEntity<Comment>(issueService.createNewCommentOnIssue(i, comment, null), HttpStatus.OK)
        : new ResponseEntity<Comment>(HttpStatus.NOT_FOUND);
  }

  @RequestMapping(value = "{owner}/{repo}/issues/{number}/actors", method = RequestMethod.GET)
  public ResponseEntity<List<OUser>> getActors(@PathVariable("owner") String owner, @PathVariable("repo") String repo,
      @PathVariable("number") Long number) {

    Issue i = organizationRepository.findSingleOrganizationIssueByRepoAndNumber(owner, repo, number);

    List<OUser> involvedActors = issueService.findInvolvedActors(i);
    for (OUser involvedActor : involvedActors) {
      userService.profileUser(SecurityHelper.currentUser(), involvedActor, owner);
    }
    return i != null ? new ResponseEntity<List<OUser>>(involvedActors, HttpStatus.OK) : new ResponseEntity<List<OUser>>(
        HttpStatus.NOT_FOUND);
  }

  @RequestMapping(value = "{owner}/{repo}/issues/{number}/comments/{comment_id}", method = RequestMethod.PATCH)
  public ResponseEntity<Comment> patchComment(@PathVariable("owner") String owner, @PathVariable("repo") String repo,
      @PathVariable("number") Long number, @PathVariable("comment_id") String commentUUID, @RequestBody Comment comment) {

    Issue i = organizationRepository.findSingleOrganizationIssueByRepoAndNumber(owner, repo, number);
    return i != null ? new ResponseEntity<Comment>(issueService.patchComment(i, commentUUID, comment), HttpStatus.OK)
        : new ResponseEntity<Comment>(HttpStatus.NOT_FOUND);
  }

  @RequestMapping(value = "{owner}/{repo}/issues/{number}/comments/{comment_id}", method = RequestMethod.DELETE)
  public ResponseEntity<Comment> deleteComment(@PathVariable("owner") String owner, @PathVariable("repo") String repo,
      @PathVariable("number") Long number, @PathVariable("comment_id") String commentUUID) {

    Issue i = organizationRepository.findSingleOrganizationIssueByRepoAndNumber(owner, repo, number);
    return i != null ? new ResponseEntity<Comment>(issueService.deleteComment(i, commentUUID, null), HttpStatus.OK)
        : new ResponseEntity<Comment>(HttpStatus.NOT_FOUND);
  }

  @RequestMapping(value = "{owner}/{repo}/teams", method = RequestMethod.GET)
  public ResponseEntity<List<OUser>> getRepositoryTeams(@PathVariable("owner") String owner, @PathVariable("repo") String repo) {
    return new ResponseEntity<List<OUser>>(organizationRepository.findTeamMembers(owner, repo), HttpStatus.OK);
  }

  @RequestMapping(value = "{owner}/{repo}/labels", method = RequestMethod.GET)
  public ResponseEntity<List<Label>> getRepositoryLabels(@PathVariable("owner") String owner, @PathVariable("repo") String repo) {
    return new ResponseEntity<List<Label>>(organizationRepository.findRepoLabels(owner, repo), HttpStatus.OK);
  }

  @RequestMapping(value = "{owner}/{repo}/milestones", method = RequestMethod.GET)
  public ResponseEntity<List<Milestone>> getRepositoryMilestones(@PathVariable("owner") String owner,
      @PathVariable("repo") String repo) {
    return new ResponseEntity<List<Milestone>>(organizationRepository.findRepoMilestones(owner, repo), HttpStatus.OK);
  }

  @RequestMapping(value = "{owner}/{repo}/scopes", method = RequestMethod.GET)
  @ResponseStatus(HttpStatus.OK)
  public List<Scope> findScopes(@PathVariable("repo") String repo) {
    return repoRepository.findScopes(repo);
  }
}

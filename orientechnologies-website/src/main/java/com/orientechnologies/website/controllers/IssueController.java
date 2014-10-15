package com.orientechnologies.website.controllers;

import com.orientechnologies.website.services.IssueImporterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@EnableAutoConfiguration
public class IssueController {

  @Autowired
  public IssueImporterService issueImporterService;

  @RequestMapping(value = "/issues/import", method = RequestMethod.GET)
  public void importIssue() {
    issueImporterService.importFromGitHub();
  }
}

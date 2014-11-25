package com.orientechnologies.website.hateoas.assembler;

import org.springframework.hateoas.ResourceAssembler;

import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.web.hateoas.IssueResource;
import org.springframework.stereotype.Component;

/**
 * Created by Enrico Risa on 25/11/14.
 */

@Component
public class IssueAssembler implements ResourceAssembler<Issue, IssueResource> {

  @Override
  public IssueResource toResource(Issue issue) {
    return new IssueResource(issue);
  }
}

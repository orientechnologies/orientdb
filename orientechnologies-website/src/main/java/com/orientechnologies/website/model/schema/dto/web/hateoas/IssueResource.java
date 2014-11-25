package com.orientechnologies.website.model.schema.dto.web.hateoas;

import com.orientechnologies.website.model.schema.dto.Issue;
import org.springframework.hateoas.Resource;

/**
 * Created by Enrico Risa on 25/11/14.
 */
public class IssueResource extends Resource<Issue> {

  public IssueResource(Issue issue) {
    super(issue);
  }
}

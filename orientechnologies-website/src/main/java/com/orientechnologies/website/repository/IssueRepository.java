package com.orientechnologies.website.repository;

import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.OUser;

import java.util.List;

/**
 * Created by Enrico Risa on 24/10/14.
 */
public interface IssueRepository extends BaseRepository<Issue> {
    List<OUser> findInvolvedActors(Issue issue);
}

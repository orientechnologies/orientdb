package com.orientechnologies.website.repository;

import com.orientechnologies.website.model.schema.dto.Event;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.IssueEvent;

/**
 * Created by Enrico Risa on 06/11/14.
 */
public interface EventRepository extends BaseRepository<Event> {
  Issue findIssueByEvent(IssueEvent event);

  IssueEvent reload(IssueEvent event);
}

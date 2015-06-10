package com.orientechnologies.website.services;

import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.Label;
import com.orientechnologies.website.model.schema.dto.OUser;

/**
 * Created by Enrico Risa on 08/06/15.
 */
public interface EventService {


    public void fireEvent(Issue issue, OUser actor, String event, OUser assignee, Label l);
}

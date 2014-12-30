package com.orientechnologies.website.services;


import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.OUser;

/**
 * Created by Enrico Risa on 30/12/14.
 */
public interface AutoAssignService {
    public void findAssignee(Issue issue, AutoAssign listener);

    public interface AutoAssign {
        public void assign(OUser actor, OUser assignee);
    }
}

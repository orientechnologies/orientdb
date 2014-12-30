package com.orientechnologies.website.services.impl;

import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.model.schema.dto.OUser;
import com.orientechnologies.website.model.schema.dto.Scope;
import com.orientechnologies.website.repository.OrganizationRepository;
import com.orientechnologies.website.services.AutoAssignService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by Enrico Risa on 30/12/14.
 */
@Service
public class AutoAssignServiceImpl implements AutoAssignService {

    @Autowired
    protected OrganizationRepository repository;

    @Override
    public void findAssignee(Issue issue, AutoAssign listener) {
        Scope scope = issue.getScope();
        if (scope != null) {
            OUser user = scope.getOwner();

            if (user == null && scope.getMembers().size() > 0) {
                user = scope.getMembers().iterator().next();
            }
            listener.assign(repository.findOwnerByName(issue.getRepository().getOrganization().getName()), user);
        }
    }
}

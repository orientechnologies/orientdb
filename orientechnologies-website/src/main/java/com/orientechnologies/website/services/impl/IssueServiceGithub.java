package com.orientechnologies.website.services.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.github.GRepo;
import com.orientechnologies.website.github.GitHub;
import com.orientechnologies.website.helper.SecurityHelper;
import com.orientechnologies.website.model.schema.dto.*;
import com.orientechnologies.website.services.IssueService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Enrico Risa on 21/11/14.
 */
public class IssueServiceGithub implements IssueService {

    private IssueServiceImpl issueService;

    public IssueServiceGithub(IssueServiceImpl issueService) {

        this.issueService = issueService;
    }

    @Override
    public void commentIssue(Issue issue, Comment comment) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Comment createNewCommentOnIssue(Issue issue, Comment comment) {
        GitHub github = new GitHub(SecurityHelper.currentToken());

        ODocument doc = new ODocument();

        String iPropertyValue = issue.getRepository().getOrganization().getName() + "/" + issue.getRepository().getName();
        doc.field("full_name", iPropertyValue);
        try {
            GRepo repo = github.repo(iPropertyValue, doc.toJSON());
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode node = mapper.createObjectNode();
            node.put("body", comment.getBody());
            String value = mapper.writeValueAsString(node);
            repo.commentIssue(issue.getNumber(), value);
        } catch (IOException e) {

        }
        return null;
    }

    @Override
    public void changeMilestone(Issue issue, Milestone milestone, OUser actor, boolean fire) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeLabels(Issue issue, List<Label> labels, boolean replace) {

    }

    @Override
    public List<Label> addLabels(Issue issue, List<String> labels, OUser actor, boolean fire, boolean remote) {
        GitHub github = new GitHub(SecurityHelper.currentToken());

        ODocument doc = new ODocument();

        String iPropertyValue = issue.getRepository().getOrganization().getName() + "/" + issue.getRepository().getName();
        doc.field("full_name", iPropertyValue);
        try {
            GRepo repo = github.repo(iPropertyValue, doc.toJSON());
            ObjectMapper mapper = new ObjectMapper();
            String value = mapper.writeValueAsString(labels);
            repo.changeIssueLabels(issue.getNumber(), value);
        } catch (IOException e) {

        }
        return new ArrayList<Label>();
    }

    @Override
    public void removeLabel(Issue issue, String label, OUser actor, boolean remote) {

        GitHub github = new GitHub(SecurityHelper.currentToken());

        ODocument doc = new ODocument();

        String iPropertyValue = issue.getRepository().getOrganization().getName() + "/" + issue.getRepository().getName();
        doc.field("full_name", iPropertyValue);
        try {
            GRepo repo = github.repo(iPropertyValue, doc.toJSON());
            repo.removeIssueLabel(issue.getNumber(), label);
        } catch (IOException e) {

        }
    }

    @Override
    public void fireEvent(Issue issueDto, Event e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeUser(Issue issue, OUser user) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void assign(Issue issue, OUser assignee, OUser actor, boolean fire) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unassign(Issue issue, OUser assignee, OUser actor, boolean fire) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeVersion(Issue issue, Milestone milestone) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changePriority(Issue issue, Priority priority) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Issue changeState(Issue issue, String state, OUser actor, boolean fire) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Issue synchIssue(Issue issue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearEvents(Issue issue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeScope(Issue issue, Scope scope) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeClient(Issue issue, Client client) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void changeEnvironment(Issue issue, Environment e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Comment patchComment(Issue issue, String commentUUID, Comment comment) {

        GitHub github = new GitHub(SecurityHelper.currentToken());

        ODocument doc = new ODocument();

        String iPropertyValue = issue.getRepository().getOrganization().getName() + "/" + issue.getRepository().getName();
        doc.field("full_name", iPropertyValue);
        try {
            GRepo repo = github.repo(iPropertyValue, doc.toJSON());
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode node = mapper.createObjectNode();
            node.put("body", comment.getBody());
            String value = mapper.writeValueAsString(node);
            repo.patchComment(issue.getNumber(), comment.getCommentId(), value);
        } catch (IOException e) {

        }
        return null;
    }

    @Override
    public Comment deleteComment(Issue issue, String commentUUID, Comment comment) {
        GitHub github = new GitHub(SecurityHelper.currentToken());

        ODocument doc = new ODocument();

        String iPropertyValue = issue.getRepository().getOrganization().getName() + "/" + issue.getRepository().getName();
        doc.field("full_name", iPropertyValue);
        try {
            GRepo repo = github.repo(iPropertyValue, doc.toJSON());
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode node = mapper.createObjectNode();
            node.put("body", comment.getBody());
            String value = mapper.writeValueAsString(node);
            repo.deleteComment(issue.getNumber(), comment.getCommentId(), value);
        } catch (IOException e) {

        }
        return null;
    }
}

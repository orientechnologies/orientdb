package com.orientechnologies.website.daemon;

import com.orientechnologies.website.OrientDBFactory;
import com.orientechnologies.website.repository.OrganizationRepository;
import com.orientechnologies.website.services.IssueService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Created by Enrico Risa on 15/05/15.
 */
@Component
public class IssueAlignDaemon {

  @Autowired
  private OrientDBFactory        dbFactory;

  @Autowired
  private IssueService           issueService;

  @Autowired
  private OrganizationRepository organizationRepository;

  @Scheduled(fixedDelay = 60 * 1000)
  public void alignIssues() {

//    OrientGraph db = dbFactory.getGraph();
//
//    ORecordIteratorClass<ODocument> oDocuments = db.getRawGraph().browseClass(Issue.class.getSimpleName());
//
//    while (oDocuments.hasNext()) {
//
//      ODocument doc = oDocuments.next();
//
//      Issue issue = OIssue.IID.fromDoc(doc, db);
//
//      List<OUser> bots = organizationRepository.findBots(issue.getRepository().getOrganization().getName());
//      if (bots.size() > 0) {
//        if (issue.getNumber() != null) {
//          OUser next = bots.iterator().next();
//          if (issueService.isChanged(issue, next)) {
//            issueService.synchIssue(issue, next);
//          }
//        }
//      }
//    }

  }
}

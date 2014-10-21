package com.orientechnologies.website.repository.impl;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.model.schema.OSiteSchema;
import com.orientechnologies.website.model.schema.dto.Repository;
import com.orientechnologies.website.repository.RepositoryRepository;

/**
 * Created by Enrico Risa on 21/10/14.
 */
@org.springframework.stereotype.Repository
public class RepositoryRepositoryImpl extends OrientBaseRepository<Repository> implements RepositoryRepository {

  @Override
  public ODocument toDoc(Repository entity) {
    ODocument doc = null;
    if (entity.getId() == null) {
      doc = new ODocument(OSiteSchema.Repository.class.getSimpleName());
    } else {
      doc = dbFactory.getGraph().getRawGraph().load(new ORecordId(entity.getId()));
    }
    doc.field(OSiteSchema.Repository.CODENAME.toString(), entity.getCodename());
    doc.field(OSiteSchema.Repository.NAME.toString(), entity.getName());
    doc.field(OSiteSchema.Repository.DESCRIPTION.toString(), entity.getDescription());
    return doc;
  }

  @Override
  public Repository fromDoc(ODocument doc) {
    Repository repo = new Repository();
    repo.setCodename((String) doc.field(OSiteSchema.Repository.CODENAME.toString()));
    repo.setDescription((String) doc.field(OSiteSchema.Repository.DESCRIPTION.toString()));
    repo.setId(doc.getIdentity().toString());
    return repo;
  }

  @Override
  public Class<Repository> getEntityClass() {
    return Repository.class;
  }
}

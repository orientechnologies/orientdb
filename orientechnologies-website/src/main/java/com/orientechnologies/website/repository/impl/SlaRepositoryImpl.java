package com.orientechnologies.website.repository.impl;

import com.orientechnologies.website.model.schema.HasSla;
import com.orientechnologies.website.model.schema.OTypeHolder;
import com.orientechnologies.website.model.schema.dto.Sla;
import com.orientechnologies.website.repository.SlaRepository;
import org.springframework.stereotype.Repository;

/**
 * Created by Enrico Risa on 05/12/14.
 */

@Repository
public class SlaRepositoryImpl extends OrientBaseRepository<Sla> implements SlaRepository {
  @Override
  public OTypeHolder<Sla> getHolder() {
    return HasSla.RANGE;
  }

  @Override
  public Class<Sla> getEntityClass() {
    return Sla.class;
  }
}

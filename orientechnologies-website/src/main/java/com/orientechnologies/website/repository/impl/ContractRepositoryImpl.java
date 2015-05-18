package com.orientechnologies.website.repository.impl;

import com.orientechnologies.website.model.schema.OContract;
import com.orientechnologies.website.model.schema.OTypeHolder;
import com.orientechnologies.website.model.schema.dto.Contract;
import com.orientechnologies.website.repository.ContractRepository;
import org.springframework.stereotype.Repository;

/**
 * Created by Enrico Risa on 12/05/15.
 */
@Repository
public class ContractRepositoryImpl extends OrientBaseRepository<Contract> implements ContractRepository {
  @Override
  public OTypeHolder<Contract> getHolder() {
    return OContract.NAME;
  }

  @Override
  public Class<Contract> getEntityClass() {
    return Contract.class;
  }
}

package com.orientechnologies.website.repository;

import com.orientechnologies.website.model.schema.dto.Contract;

/**
 * Created by Enrico Risa on 12/05/15.
 */
public interface ContractRepository extends BaseRepository<Contract> {
  Contract findByName(String orgName, String contractName);

  public Contract findByUUID(String orgName, String uuid);
}

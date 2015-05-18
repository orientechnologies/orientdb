package com.orientechnologies.website.services;

import com.orientechnologies.website.model.schema.dto.Contract;
import com.orientechnologies.website.model.schema.dto.Priority;

import java.util.Date;

/**
 * Created by Enrico Risa on 11/05/15.
 */
public interface SlaService {

  public Date calculateDueTime(Date now, Contract contract, Priority priority);


}

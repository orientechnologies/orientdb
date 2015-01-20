package com.orientechnologies.website.controllers;

import com.orientechnologies.website.exception.ServiceException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;

/**
 * Created by Enrico Risa on 20/01/15.
 */
public class ExceptionController {

  @ExceptionHandler
  public ResponseEntity<String> handlServiceException(ServiceException e) {
    return new ResponseEntity<String>(e.toJson(), HttpStatus.BAD_REQUEST);
  }
}

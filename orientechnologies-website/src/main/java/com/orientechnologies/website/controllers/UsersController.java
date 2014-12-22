package com.orientechnologies.website.controllers;

import com.orientechnologies.website.helper.SecurityHelper;
import com.orientechnologies.website.model.schema.dto.Environment;
import com.orientechnologies.website.model.schema.dto.web.UserDTO;
import com.orientechnologies.website.services.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@EnableAutoConfiguration
@RequestMapping(ApiUrls.ROOT_V1)
public class UsersController {

  @Autowired
  protected UserService userService;

  @RequestMapping("user")
  public UserDTO currentUser() {
    return userService.forWeb(SecurityHelper.currentUser());
  }

  @RequestMapping(value = "users/{username}/environments", method = RequestMethod.POST)
  public Environment registerUserEnvironment(@PathVariable("username") String username, @RequestBody Environment environment) {
    return userService.registerUserEnvironment(SecurityHelper.currentUser(), environment);
  }

  @RequestMapping(value = "users/{username}/environments", method = RequestMethod.GET)
  public List<Environment> getUserEnvironments(@PathVariable("username") String username) {
    return userService.getUserEnvironments(SecurityHelper.currentUser());
  }

  @RequestMapping(value = "users/{username}/environments/{id}", method = RequestMethod.DELETE)
  public ResponseEntity<Environment> deleteUserEnvironments(@PathVariable("username") String username, @PathVariable("id") Long id) {
    userService.deregisterUserEnvironment(SecurityHelper.currentUser(), id);
    return new ResponseEntity<Environment>(HttpStatus.OK);
  }

  @RequestMapping(value = "users/{username}/environments/{id}", method = RequestMethod.PATCH)
  public Environment patchUserEnvironments(@PathVariable("username") String username, @PathVariable("id") Long id,
      @RequestBody Environment environment) {
    return userService.patchUserEnvironment(SecurityHelper.currentUser(), id, environment);
  }
}

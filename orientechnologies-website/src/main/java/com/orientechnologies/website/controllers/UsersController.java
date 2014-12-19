package com.orientechnologies.website.controllers;

import com.orientechnologies.website.helper.SecurityHelper;
import com.orientechnologies.website.model.schema.dto.Environment;
import com.orientechnologies.website.model.schema.dto.web.UserDTO;
import com.orientechnologies.website.services.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

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
  public Environment registerUserEnvironment(@PathVariable("username") String username, Environment environment) {
    return userService.registerUserEnvironment(SecurityHelper.currentUser(), environment);
  }
}

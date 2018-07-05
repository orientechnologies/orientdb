package com.orientechnologies.website.controllers;

import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.website.exception.ServiceException;
import com.orientechnologies.website.helper.SecurityHelper;
import com.orientechnologies.website.model.schema.dto.Environment;
import com.orientechnologies.website.model.schema.dto.UserChangePassword;
import com.orientechnologies.website.model.schema.dto.UserRegistration;
import com.orientechnologies.website.model.schema.dto.UserToken;
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
  public ResponseEntity<Environment> deleteUserEnvironments(@PathVariable("username") String username,
      @PathVariable("id") Long id) {
    userService.deregisterUserEnvironment(SecurityHelper.currentUser(), id);
    return new ResponseEntity<Environment>(HttpStatus.OK);
  }

  @RequestMapping(value = "users/{username}/environments/{id}", method = RequestMethod.PATCH)
  public Environment patchUserEnvironments(@PathVariable("username") String username, @PathVariable("id") Long id,
      @RequestBody Environment environment) {
    return userService.patchUserEnvironment(SecurityHelper.currentUser(), id, environment);
  }

  @RequestMapping(value = "users/{username}", method = RequestMethod.PATCH)
  public com.orientechnologies.website.model.schema.dto.OUser patchUser(@PathVariable("username") String username,
      @RequestBody UserDTO user) {

    return userService.patchUser(SecurityHelper.currentUser(), user);
  }

  @RequestMapping(value = "users/{username}/changePassword", method = RequestMethod.POST)
  public ResponseEntity<?> changePassword(@PathVariable("username") String username, @RequestBody UserChangePassword user) {
    userService.changePassword(SecurityHelper.currentUser(), user);
    return new ResponseEntity(HttpStatus.OK);
  }

  @RequestMapping(value = "users/{username}/resetPassword", method = RequestMethod.POST)
  public ResponseEntity<?> resetPassword(@PathVariable("username") String username, @RequestBody UserChangePassword user) {
    userService.changePasswordWithToken(SecurityHelper.currentUser(), user);
    return new ResponseEntity(HttpStatus.OK);
  }

  @RequestMapping(value = "users", method = RequestMethod.POST)
  public ResponseEntity<?> createUser(@RequestBody UserRegistration user) {

    userService.registerUser(user);

    return new ResponseEntity<>(HttpStatus.CREATED);
  }

  @RequestMapping(value = "login", method = RequestMethod.POST)
  public ResponseEntity<UserToken> loginUser(@RequestBody UserRegistration user) {

    return new ResponseEntity<>(userService.login(user), HttpStatus.OK);
  }

  @RequestMapping(value = "resetPassword", method = RequestMethod.POST)
  public ResponseEntity<?> resetPassword(@RequestBody UserRegistration user) {

    userService.resetPassword(user);
    return new ResponseEntity<>(HttpStatus.OK);
  }

  @RequestMapping(value = "validateToken", method = RequestMethod.POST)
  public ResponseEntity<UserToken> validateToken(@RequestBody UserToken user) {

    return new ResponseEntity<UserToken>(userService.validateToken(user), HttpStatus.OK);
  }

  @ExceptionHandler({ ORecordDuplicatedException.class })
  public ResponseEntity<String> handleDuplicateException() {

    return new ResponseEntity(ServiceException.create(400, "The username is taken. Please chose another.").toJson(),
        HttpStatus.BAD_REQUEST);
  }

}

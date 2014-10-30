package com.orientechnologies.website.controllers;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * Created by Enrico Risa on 30/10/14.
 */
@Controller
public class HomeController {

  @RequestMapping("/home")
  public String home() {
    return "forward:/index.html";
  }
}

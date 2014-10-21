package com.orientechnologies.website.controllers;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import com.orientechnologies.website.configuration.ApiVersion;
import com.orientechnologies.website.services.DeveloperService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import com.jcabi.github.Github;
import com.jcabi.github.RtGithub;
import com.jcabi.github.User;
import com.orientechnologies.website.configuration.GitHubConfiguration;

/**
 * Created by Enrico Risa on 20/10/14.
 */
@RestController
@EnableAutoConfiguration
@RequestMapping("github")
@ApiVersion(1)
public class GithubLoginController {

  @Autowired
  private GitHubConfiguration gitHubConfiguration;

  @Autowired
  private DeveloperService    developerService;

  @RequestMapping(value = "/login", method = RequestMethod.GET)
  public RedirectView login() {

    String baseUrl = gitHubConfiguration.getLoginUrl() + "/authorize?";
    String s = "client_id=" + gitHubConfiguration.getClientId();
    String locationUrl = baseUrl + s + "&scope=user,read:org";
    RedirectView view = new RedirectView();
    view.setUrl(locationUrl);
    return view;
  }

  @RequestMapping(value = "/authorize", method = RequestMethod.GET, params = { "code" })
  public RedirectView authorize(@RequestParam("code") String code) {

    String locationUrl = gitHubConfiguration.getLoginUrl() + "/access_token?client_id=" + gitHubConfiguration.getClientId()
        + "&client_secret=" + gitHubConfiguration.getClientSecret() + "&code=" + code;

    try {
      URL obj = new URL(locationUrl);

      HttpURLConnection connection = (HttpURLConnection) obj.openConnection();
      connection.setRequestMethod("POST");
      connection.connect();
      InputStream is = connection.getInputStream();
      InputStreamReader isr = new InputStreamReader(is);

      int numCharsRead;
      char[] charArray = new char[1024];
      StringBuffer sb = new StringBuffer();
      while ((numCharsRead = isr.read(charArray)) > 0) {
        sb.append(charArray, 0, numCharsRead);
      }
      String response = sb.toString();
      String authKey = response.split("&")[0].split("=")[1];
      developerService.initUser(authKey);
    } catch (java.io.IOException e) {
      e.printStackTrace();
    }

    RedirectView view = new RedirectView();
    view.setUrl("/");
    return view;
  }

}

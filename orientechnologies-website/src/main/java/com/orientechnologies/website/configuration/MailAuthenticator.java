package com.orientechnologies.website.configuration;

import javax.mail.Authenticator;
import javax.mail.PasswordAuthentication;

public class MailAuthenticator extends Authenticator {
  private String username = "prjhub@orientechnologies.com";
  private String password = "Prj_Hubb_WoW945";
  public MailAuthenticator() {
    super();
  }

  @Override
  protected PasswordAuthentication getPasswordAuthentication() {
    return  new PasswordAuthentication(username,password);
  }
}

import com.orientechnologies.website.configuration.MailAuthenticator;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

public class SendMail {

  public static void main(String[] args) throws Exception {
    Authenticator auth = new MailAuthenticator();
    Properties props = new Properties();
    props.put("mail.smtp.host", "mail.orientechnologies.com");
    props.put("mail.smtp.port", "587");
    props.put("mail.smtp.starttls.enable", "true");
    props.put("mail.smtp.auth", "true");
    props.put("mail.smtp.ssl.trust", "*");

    Session session = Session.getInstance(props, auth);
    // session.setDebug(true);

    try {

      Message message = new MimeMessage(session);
      message.setFrom(new InternetAddress("prjhub@orientechnologies.com"));
      message.setRecipients(Message.RecipientType.TO, InternetAddress.parse("enrico.risa@gmail.com"));
      message.setSubject("Test");
      message.setText("Ciao");

      Transport.send(message);

    } catch (MessagingException e) {
      e.printStackTrace();
    }
  }
}

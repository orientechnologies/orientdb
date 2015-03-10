package com.orientechnologies.website.events;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import reactor.event.Event;

import com.orientechnologies.website.model.schema.dto.Message;
import com.orientechnologies.website.websocket.ChatHandler;

/**
 * Created by Enrico Risa on 21/01/15.
 */

@Component
public class ChatMessageEditEvent extends EventInternal<Message> {

  public static String  EVENT = "message_edit";

  @Autowired
  protected ChatHandler chatHandler;

  @Override
  public String event() {
    return EVENT;
  }

  @Override
  public void accept(Event<Message> messageEvent) {

    Message message = messageEvent.getData();
    chatHandler.broadcast(message.getClientId(), message,true);
  }
}

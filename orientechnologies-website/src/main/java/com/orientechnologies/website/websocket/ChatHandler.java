package com.orientechnologies.website.websocket;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.model.schema.dto.Message;
import com.orientechnologies.website.repository.MessageRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Enrico Risa on 20/01/15.
 */

@Component
public class ChatHandler extends TextWebSocketHandler {

  protected Map<Integer, Set<WebSocketSession>> rooms      = new ConcurrentHashMap<Integer, Set<WebSocketSession>>();

  protected Map<Integer, Date>                  timestamps = new ConcurrentHashMap<Integer, Date>();

  @Autowired
  protected MessageRepository                   messageRepository;

  @Override
  protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {

    ODocument msg = new ODocument().fromJSON(message.getPayload());
    if ("join".equals(msg.field("action"))) {
      List<Integer> roomsIds = msg.field("rooms");

      for (Integer roomsId : roomsIds) {
        Set<WebSocketSession> sessions = rooms.get(roomsId);
        if (sessions == null) {
          sessions = new HashSet<WebSocketSession>();
          rooms.put(roomsId, sessions);
        }
        sessions.add(session);
      }
    }
  }

  public void broadcast(Integer roomId, Message message,boolean edited) {
    ObjectMapper mapper = new ObjectMapper();
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    message.setEdited(edited);
    try {
      mapper.writeValue(stream, message);

      Set<WebSocketSession> sessions = rooms.get(roomId);
      timestamps.put(roomId, message.getDate());
      if (sessions != null) {
        for (WebSocketSession session : sessions) {
          session.sendMessage(new TextMessage(stream.toByteArray()));
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
    for (Integer integer : rooms.keySet()) {
      Set<WebSocketSession> sessions = rooms.get(integer);
      if (sessions != null) {
        sessions.remove(session);
      }
    }
  }

  public Date getRoomTimestamp(Integer room) {
    Date date = timestamps.get(room);
    if (date == null) {
      Message m = messageRepository.getLastMessage(room);
      if (m != null) {
        date = m.getDate();
        timestamps.put(room, date);
      }
    }
    return timestamps.get(room);
  }
}

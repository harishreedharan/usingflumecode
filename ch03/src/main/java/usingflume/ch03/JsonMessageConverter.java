package usingflume.ch03;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.event.JSONEvent;
import org.apache.flume.source.jms.JMSMessageConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.TextMessage;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class JsonMessageConverter implements JMSMessageConverter,
  Configurable {

  private static final Logger LOGGER =
    LoggerFactory.getLogger(JsonMessageConverter.class);
  private final Type listType
    = new TypeToken<List<JSONEvent>>() {
  }.getType();
  private final Gson gson
    = new GsonBuilder().disableHtmlEscaping().create();
  private String charset = "UTF-8";

  @Override
  public List<Event> convert(javax.jms.Message message)
    throws JMSException {

    Preconditions.checkState(message instanceof TextMessage,
      "Expected a text nessage, gut the message received " +
        "was not Text");
    List<JSONEvent> events =
      gson.fromJson(((TextMessage) message).getText(), listType);
    return convertToNormalEvents(events);
  }

  private List<Event> convertToNormalEvents(List<JSONEvent> events) {
    List<Event> newEvents = new ArrayList<Event>(events.size());
    for(JSONEvent e : events) {
      e.setCharset(charset);
      newEvents.add(EventBuilder.withBody(e.getBody(),
        e.getHeaders()));
    }
    return newEvents;
  }

  @Override
  public void configure(Context context) {
    try {
    charset = context.getString("charset", "UTF-8");
    } catch (Exception ex) {
      LOGGER.warn("Charset not found. Using UTF-8 instead", ex);
      charset = "UTF-8";
    }

  }
}

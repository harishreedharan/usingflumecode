package usingflume.ch03;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.flume.event.JSONEvent;
import org.apache.flume.source.jms.JMSMessageConverter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.jms.TextMessage;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.List;
import static org.mockito.Mockito.*;

public class TestJsonMessageConverter {

  private final JMSMessageConverter converter = new JsonMessageConverter();
  private final Type listType = new TypeToken<List<JSONEvent>>() {
  }.getType();
  private final Gson gson
    = new GsonBuilder().disableHtmlEscaping().create();
  final List<JSONEvent> events = Lists.newArrayList();

  @Before
  public void setUp() {
    for(int i = 0; i < 100; i++) {
      final JSONEvent event = new JSONEvent();
      event.setBody(getEventBody(i));
      events.add(event);
    }
  }

  private byte[] getEventBody(int i) {
    return ("test - " + i).getBytes(Charset.forName("UTF-8"));
  }


  private TextMessage getMessages() throws Exception {
    final String TEST = gson.toJson(events, listType);
    final TextMessage msg = mock(TextMessage.class);
    when(msg.getText()).thenReturn(TEST);
    return msg;
  }

  @Test
  public void testSingleEvent() throws Exception {
    converter.convert(getMessages());
    for (int i = 0; i < 100; i++) {
      final byte[] eventBodyBytes = events.get(i).getBody();
      System.out.println(new String(eventBodyBytes, Charset.forName("UTF-8")));
      Assert.assertArrayEquals(events.get(i).getBody(), getEventBody(i));
    }
  }
}
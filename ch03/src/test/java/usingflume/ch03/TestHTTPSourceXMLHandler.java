package usingflume.ch03;

import junit.framework.Assert;
import org.apache.commons.codec.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class TestHTTPSourceXMLHandler {

  private List<Event> events = new ArrayList<Event>();

  private final Boolean insertTimestamp;

  public TestHTTPSourceXMLHandler(Boolean insertTimestamp, Boolean ignore) {
    this.insertTimestamp = insertTimestamp;
  }

  @Before
  public void setUp() {
    Map<String, String> headers1 = new HashMap<String, String>();
    headers1.put("header1", "value1");
    headers1.put("header2", "value2");

    events.add(EventBuilder.withBody(("This is a test. This input should " +
      "show up in an event.").getBytes(Charsets.UTF_8), headers1));

    Map<String, String> headers2 = new HashMap<String, String>();
    headers2.put("event2Header1", "event2Value1");
    headers2.put("event2Header2", "event2Value2");
    events.add(EventBuilder.withBody((
      "This is the 2nd event.").getBytes(Charsets.UTF_8), headers1));
  }

  @Parameterized.Parameters
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[][]{
      {true, null}, {false, null}
    });
  }

  @Test
  public void testHTTPSourceXMLHandler() throws Exception {
    HTTPSourceXMLHandler handler = new HTTPSourceXMLHandler();
    Context ctx = new Context();
    ctx.put("insertTimestamp", String.valueOf(insertTimestamp));
    handler.configure(ctx);
    int index = 0;
    List<Event> events = handler.getEvents(new UsingFlumeServletRequest());
    Assert.assertEquals(2, events.size());
    for(Event e : events) {
      Event orig = events.get(index++);
      Map<String, String> origHeaders = orig.getHeaders();
      Map<String, String> headers = e.getHeaders();
      if(insertTimestamp) {
        Assert.assertNotNull(headers.remove("timestamp"));
      }
      for(String key : headers.keySet()) {
        Assert.assertEquals(origHeaders.get(key), headers.get(key));
      }
      Assert.assertEquals(new String(orig.getBody(), "UTF-8"),
        new String(e.getBody(), "UTF-8"));
    }
  }
}

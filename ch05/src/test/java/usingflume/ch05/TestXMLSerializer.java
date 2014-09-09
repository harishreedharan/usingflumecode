package usingflume.ch05;/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.serialization.EventSerializerFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestXMLSerializer {

  private List<Event> events = new ArrayList<Event>();
  private String expectedOutput;


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
      "This is the 2nd event.").getBytes(Charsets.UTF_8), headers2));

    StringBuilder builder = new StringBuilder();
    builder.append("<events>\n")
      .append("<event>\n")
      .append("<headers>\n")
      .append("<header1>").append("value1").append("</header1>\n")
      .append("<header2>").append("value2").append("</header2>\n")
      .append("</headers>\n")
      .append("<body>").append("This is a test. This input should " +
      "show up in an event.").append("</body>\n")
      .append("</event>\n")
      .append("<event>\n")
      .append("<headers>\n")
      .append("<event2Header1>").append("event2Value1").append
      ("</event2Header1>\n")
      .append("<event2Header2>").append("event2Value2").append
      ("</event2Header2>\n")
      .append("</headers>\n")
      .append("<body>").append("This is the 2nd event.").append("</body>\n")
      .append("</event>\n").append("</events>");
    expectedOutput = builder.toString();
  }

  @Test
  public void test() throws IOException {
    Context ctx = new Context();
    ctx.put("charset", "UTF-8");
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    EventSerializerFactory factory = new EventSerializerFactory();
    EventSerializer serializer =
      factory.getInstance(XMLSerializer.Builder.class.getName(), ctx, os);
    serializer.afterCreate();
    for(Event e : events) {
      serializer.write(e);
    }
    serializer.flush();
    serializer.beforeClose();
    Assert.assertEquals(expectedOutput, new String(os.toByteArray(),
      Charsets.UTF_8));
  }
}

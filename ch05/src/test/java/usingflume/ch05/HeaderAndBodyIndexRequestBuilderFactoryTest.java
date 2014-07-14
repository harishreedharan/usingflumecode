/*
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
package usingflume.ch05;

import com.google.common.collect.Maps;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.sink.elasticsearch
  .ElasticSearchIndexRequestBuilderFactory;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.node.NodeBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class HeaderAndBodyIndexRequestBuilderFactoryTest {
  private static final String EVENT_ID = "EventId";
  private static final String IDX_PREFIX = "usingFlume-";
  private static final String IDX_TYPE_PREFIX = "movies-";
  private static final String TEST_EVENT_PREFIX = "Test Event ";
  private Context ctx = new Context();

  @Test
  public void testPrepareIndexRequest() throws Exception {
    ElasticSearchIndexRequestBuilderFactory factory = new
      HeaderAndBodyIndexRequestBuilderFactory();
    // There is a small race condition here.
    // If you start the test before midnight and continue after,
    // then this might fail because the date has changed.
    FastDateFormat date =
      FastDateFormat.getDateInstance(FastDateFormat.FULL);

    String suffix = date.format(System.currentTimeMillis());

    final Client client = NodeBuilder.nodeBuilder().client(true).local(true)
      .node().client();
    ctx.put("writeHeaders", "true");
    factory.configure(ctx);
    for (int i = 0; i < 10; i++) {
      final String iStr = String.valueOf(i);
      final String BODY_STR = TEST_EVENT_PREFIX + iStr;
      final String IDX_NAME = IDX_PREFIX + iStr;
      final String IDX_TYPE = IDX_TYPE_PREFIX + iStr;

      final Map<String, String> headers = Maps.newHashMap();
      headers.put(EVENT_ID, iStr);

      final Event e = EventBuilder.withBody(BODY_STR.getBytes(Charsets.UTF_8),
        headers);
      final IndexRequestBuilder request = factory.createIndexRequest(client,
        IDX_NAME, IDX_TYPE, e);
      final Map src = request.request().sourceAsMap();
      Assert.assertEquals(src.get(EVENT_ID), iStr);
      Assert.assertEquals(src.get("body"), BODY_STR);
      Assert.assertTrue(request.request().type().startsWith(IDX_TYPE));
      Assert.assertTrue(request.request().index().startsWith(IDX_NAME));
      Assert.assertTrue(request.request().index().endsWith(suffix));
    }

    ctx.put("writeHeaders", "false");
    factory.configure(ctx);

    final IndexRequestBuilder request = factory.createIndexRequest(client,
      IDX_PREFIX, IDX_TYPE_PREFIX, EventBuilder.withBody("test data",
      Charsets.UTF_8));
    byte[] body = request.request().source().array();
    Assert.assertEquals("test data", new String(body, Charsets.UTF_8));
    Assert.assertTrue(request.request().type().startsWith(IDX_TYPE_PREFIX));
    Assert.assertTrue(request.request().index().startsWith(IDX_PREFIX));
    Assert.assertTrue(request.request().index().endsWith(suffix));
  }
}

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
package usingflume.ch03;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.flume.Context;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

public class TestNettyMessageSource {

  @Ignore
  @Test
  public void testBasicFunctionality() throws Exception {
    Channel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());
    channel.start();
    NettyMessageSource src = new NettyMessageSource();
    Context ctx = new Context();
    ctx.put("host", "0.0.0.0");
    ctx.put("port", "41414");
    Configurables.configure(src, ctx);
    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);
    ChannelProcessor processor = new ChannelProcessor(rcs);
    src.setChannelProcessor(processor);
    src.start();

    List<String> eventList = Lists.newArrayList();
    SocketChannel socket = SocketChannel.open();
    socket.connect(new InetSocketAddress("0.0.0.0", 41414));
    ByteBuffer byteBuffer = ByteBuffer.allocate(100);
    for(int i = 0; i < 10; i++) {
      byteBuffer.clear();
      String event = RandomStringUtils.random(30);
      eventList.add(event);
      byte[] eventBytes = event.getBytes(Charsets.UTF_8);
      byteBuffer.putInt(eventBytes.length);
      byteBuffer.put(eventBytes);
      byteBuffer.flip();
      socket.write(byteBuffer);
    }
    socket.close();

    Transaction tx = channel.getTransaction();
    tx.begin();
    for(int i = 0; i < 10; i++) {
      Event e = channel.take();
      Assert.assertNotNull(e);
      Assert.assertEquals(eventList.get(i), new String(e.getBody(), Charsets.UTF_8));
    }
    tx.commit();
    tx.close();

    src.stop();
  }
}

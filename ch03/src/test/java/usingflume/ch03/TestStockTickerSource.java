package usingflume.ch03;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestStockTickerSource {

  private final MemoryChannel channel = new MemoryChannel();

  @Test
  public void testStockTickerSource() throws Exception {
    final StockTickerSource source = new StockTickerSource();
    Configurables.configure(channel, new Context());
    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);
    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);
    source.setChannelProcessor(new ChannelProcessor(rcs));
    Context ctx = new Context();
    ctx.put("tickers", "FLUME1 FLUME2 FLUME3 FLUME4");
    ctx.put("refreshInterval", "1");

    Configurables.configure(source, ctx);
    source.start();
    source.process();
    Thread.sleep(2000);
    source.process();

    Transaction tx = channel.getTransaction();
    tx.begin();
    Event e;
    int count = 0;
    while((e = channel.take()) != null) {
      System.out.println(new String(e.getBody()));
      count++;
    }
    tx.commit();
    tx.close();
    Assert.assertEquals(8, count);
  }

  @After
  public void tearDown() throws Exception {

  }
}
package usingflume.ch07;

import com.google.common.collect.Lists;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Source;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.source.AvroSource;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;


public class TestUsingFlumeEmbeddedAgent {

  @Test
  public void testEmbeddedAgent() throws Exception {
    final Source source = new AvroSource();
    final MemoryChannel channel = new MemoryChannel();
    Context ctx = new Context();
    ctx.put("capacity", "100000");
    ctx.put("transactionCapacity", "100000");
    Configurables.configure(channel, ctx);
    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);
    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);
    source.setChannelProcessor(new ChannelProcessor(rcs));
    ctx.put("bind", "0.0.0.0");
    ctx.put("port", "41434");
    List<String> args = Lists.newArrayList();
    args.add("-r");
    args.add("0.0.0.0");
    args.add("-p");
    args.add("41434");
    String[] argsArray = args.toArray(new String[0]);
    Configurables.configure(source, ctx);
    channel.start();
    source.start();
    Thread.sleep(2000);
    UsingFlumeEmbeddedAgent.main(argsArray);
    Thread.sleep(5000);
    source.stop();
    Transaction tx = channel.getTransaction();
    tx.begin();
    int i = 0;
    while (channel.take() != null) {
      i++;
    }
    tx.commit();
    tx.close();

    // 5 threads, each thread 100 transactions of 100 events each
    Assert.assertEquals(10000, i);
  }
}
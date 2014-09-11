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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public class TestUsingFlumeDefaultRPCApp {

  private final boolean compress, ssl;

  @Parameterized.Parameters
  public static Collection<Object[]> inputs() {
    List<Object[]> params = Lists.newArrayList();
    params.add(new Object[]{false, false});
    params.add(new Object[]{true, false});
    params.add(new Object[]{false, true});
    params.add(new Object[]{true, true});
    return params;
  }

  public TestUsingFlumeDefaultRPCApp(boolean compress, boolean ssl) {
    this.compress = compress;
    this.ssl = ssl;
  }

  @Test
  public void testRPCClient() throws Exception {
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
    args.add("0.0.0.0:41434");
    if (compress) {
      ctx.put("compression-type", "deflate");
      args.add("-c");
    }
    if (ssl) {
      ctx.put("ssl", "true");
      ctx.put("keystore", getClass().getResource("/server.p12").getFile());
      ctx.put("keystore-password", "password");
      ctx.put("keystore-type", "PKCS12");
      args.add("-s");
      args.add("-k");
      args.add(getClass().getResource("/server.p12").getFile());
      args.add("-d");
      args.add("password");
      args.add("-t");
      args.add("PKCS12");
    }
    String[] argsArray = args.toArray(new String[0]);
    Configurables.configure(source, ctx);
    channel.start();
    source.start();
    Thread.sleep(2000);
    UsingFlumeDefaultRPCApp.main(argsArray);
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
   Assert.assertEquals(50000, i);
  }
}
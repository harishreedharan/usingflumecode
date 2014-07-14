package usingflume.ch03;

import com.google.common.base.Preconditions;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractEventDrivenSource;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio
  .NioServerSocketChannelFactory;

import java.net.InetSocketAddress;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A simple example Flume Event Driven Source that reads data on a
 * configurable interface and port.
 * <p/>
 * The message format is simple:
 * &lt;length of message in bytes&gt;&lt;message&gt;.
 * The message is used as the body of a flume event.
 * This source is meant as an example of an Event
 * driven source and not meant to be deployed for
 * regular use. No attempt is made to ensure correctness
 * of the message format, as this Source is simply
 * an example. If the length is incorrect or invalid,
 * or the message does not correspond to the the
 * length, the source might behave erratically
 * until restarted. Each message is written out in its
 * own transaction to the channel,
 * so performance with File Channel
 * might not be very good.
 */

public class NettyMessageSource extends AbstractEventDrivenSource {

  private Integer port;
  private String host;
  private ServerBootstrap server;

  @Override
  protected void doConfigure(Context context)
    throws FlumeException {
    host = context.getString("host");
    port = context.getInteger("port");
    Preconditions.checkNotNull(port, "port cannot be null for " +
      "NettyMessageSource");
    Preconditions.checkNotNull(host, "host cannot be null for " +
      "NettyMessageSource");
  }

  @Override
  protected void doStart() throws FlumeException {
    ExecutorService nettyExecutor
      = Executors.newCachedThreadPool();
    server = new ServerBootstrap(new NioServerSocketChannelFactory
      (nettyExecutor, nettyExecutor));

    server.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast(
          "usingFlumeHandler", new UsingFlumeHandler());
        return pipeline;
      }
    });
    server.bind(new InetSocketAddress(host, port));
  }

  @Override
  protected void doStop() throws FlumeException {
    server.shutdown();
  }

  private class UsingFlumeHandler extends SimpleChannelHandler {

    private final ChannelBuffer buffer
      = ChannelBuffers.dynamicBuffer();
    private int length = 0;
    private boolean lengthReceived = false;
    private boolean seqReceived = false;
    private int currentSeq = -1;

    @Override
    public void messageReceived(ChannelHandlerContext ctx,
      MessageEvent e) {
      ChannelBuffer msg = (ChannelBuffer) e.getMessage();
      buffer.writeBytes(msg);
      while (true) {
        if(!seqReceived && buffer.readableBytes() >= 4) {
          buffer.markReaderIndex();
          currentSeq = buffer.readInt();
          seqReceived = true;
        }
        if (!lengthReceived && buffer.readableBytes() >= 4) {
          length = buffer.readInt();
          lengthReceived = true;
        } else {
          return;
        }
        // length = 0 case is also handled below.
        if (buffer.readableBytes() >= length) {
          byte[] eventBody = new byte[length];
          buffer.readBytes(eventBody);
          try {
            getChannelProcessor().processEvent(
              EventBuilder.withBody(eventBody));
            ByteBuffer seq = ByteBuffer.allocate(4);
            seq.putInt(currentSeq);
            e.getChannel().write(ChannelBuffers.wrappedBuffer(seq));
            buffer.discardReadBytes();
            lengthReceived = false;
          } catch (Throwable ex) {
            seqReceived = false;
            lengthReceived = false;
            buffer.resetReaderIndex();
          }
        }
      }
    }
  }
}

package usingflume.ch03;

import com.google.common.base.Preconditions;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.source.AbstractEventDrivenSource;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.listener.ListenerFactory;

public class FtpSource extends AbstractEventDrivenSource{

  private static final String CONFIG_HOST = "host";
  private static final String CONFIG_PORT = "port";
  private String host;
  private int port;

  @Override
  protected void doConfigure(Context context) throws FlumeException {
    host = Preconditions.checkNotNull(context.getString
      (CONFIG_HOST), "Host must be specified");
    port = Preconditions.checkNotNull(context.getInteger
      (CONFIG_PORT), "Port must be specified");
  }

  @Override
  protected void doStart() throws FlumeException {
    FtpServerFactory serverFactory = new FtpServerFactory();
    ListenerFactory factory = new ListenerFactory();
    factory.setPort(port);
    serverFactory.addListener("default", factory.createListener());
    FtpServer server = serverFactory.createServer();
    try {
      server.start();
    } catch (FtpException e) {
      throw new FlumeException("Error while attempting to start FTP" +
        " Server", e);
    }

  }

  @Override
  protected void doStop() throws FlumeException {

  }
}

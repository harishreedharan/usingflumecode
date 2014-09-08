package usingflume.ch07;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import java.util.Properties;

import static org.apache.flume.api.RpcClientConfigurationConstants.*;

public class UsingFlumeDefaultRPCApp extends UsingFlumeRPCApp {

  private String host;
  private String port;

  @Override
  protected void setClient(Properties p) {
    p.setProperty(CONFIG_CLIENT_TYPE, DEFAULT_CLIENT_TYPE);
  }

  @Override
  protected void parseHostsAndPort(CommandLine commandLine,
    Properties config) {
    config.setProperty(CONFIG_HOSTS, "h1");

    host = commandLine.getOptionValue("r").trim();
    Preconditions.checkNotNull(host, "Remote host cannot be null.");

    port = commandLine.getOptionValue("p").trim();
    Preconditions.checkNotNull(port, "Port cannot be null.");
    // This becomes hosts.h1
    config.setProperty(CONFIG_HOSTS_PREFIX + "h1", host + ":" + port);
  }

  @Override
  protected void backoffConfig(CommandLine commandLine,
    Properties config) {
    // No op
  }

  public static void main(String args[]) throws ParseException {
    // Outsource all work to the app.run method which can be tested
    // more easily
    final UsingFlumeDefaultRPCApp app = new UsingFlumeDefaultRPCApp();
    app.run(args);
  }
}

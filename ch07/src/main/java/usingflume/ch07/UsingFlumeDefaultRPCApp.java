package usingflume.ch07;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;

import java.util.Properties;

import static org.apache.flume.api.RpcClientConfigurationConstants.*;

public class UsingFlumeDefaultRPCApp extends UsingFlumeRPCApp {

  private String remote;

  @Override
  protected void setClientTypeInConfig(Properties p) {
    p.setProperty(CONFIG_CLIENT_TYPE, DEFAULT_CLIENT_TYPE);
  }

  @Override
  protected void parseHostsAndPort(CommandLine commandLine,
    Properties config) {
    config.setProperty(CONFIG_HOSTS, "h1");

    remote = commandLine.getOptionValue("r").trim();
    Preconditions.checkNotNull(remote, "Remote cannot be null.");
    // This becomes hosts.h1
    config.setProperty(CONFIG_HOSTS_PREFIX + "h1", remote);
  }

  @Override
  protected void backoffConfig(CommandLine commandLine,
    Properties config) {
    // No op
  }

  public static void main(String args[]) throws Exception {
    // Outsource all work to the app.run method which can be tested
    // more easily
    final UsingFlumeDefaultRPCApp app = new UsingFlumeDefaultRPCApp();
    app.run(args);
  }
}

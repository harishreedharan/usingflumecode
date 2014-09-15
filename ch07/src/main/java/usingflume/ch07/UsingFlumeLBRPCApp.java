package usingflume.ch07;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;

import java.util.Properties;

import static org.apache.flume.api.RpcClientConfigurationConstants.*;

public class UsingFlumeLBRPCApp extends UsingFlumeRPCApp {

  private String host;
  private String port;

  @Override
  protected void setClientTypeInConfig(Properties p) {
    p.setProperty(CONFIG_CLIENT_TYPE, "default_loadbalance");
  }

  protected void parseHostsAndPort(CommandLine commandLine,
    Properties config) {
    host = commandLine.getOptionValue("r").trim();
    Preconditions.checkNotNull(host, "Remote host cannot be null.");
    StringBuilder hostBuilder = new StringBuilder("");

    String[] hostnames = host.split(",");
    int hostCount = hostnames.length;

    for (int i = 1; i <= hostCount; i++) {
      hostBuilder.append("h").append(i).append(" ");
    }
    config.setProperty(CONFIG_HOSTS, hostBuilder.toString());

    for (int i = 1; i <= hostCount; i++) {
      config.setProperty(
        CONFIG_HOSTS_PREFIX + "h" + String.valueOf(i),
        hostnames[i - 1]);
    }
  }

  @Override
  protected void backoffConfig(CommandLine commandLine,
    Properties config) {
    if (commandLine.hasOption("o")) {
      config.setProperty(CONFIG_BACKOFF, "true");
    }
  }


  public static void main(String args[]) throws Exception {
    // Outsource all work to the app.run method which can be tested
    // more easily
    final UsingFlumeLBRPCApp app = new UsingFlumeLBRPCApp();
    app.run(args);
  }
}

package usingflume.ch07;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.flume.Event;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flume.api.RpcClientConfigurationConstants.*;

public abstract class UsingFlumeRPCApp {

  private static final Logger LOGGER = LoggerFactory.getLogger(
    UsingFlumeRPCApp.class);

  private RpcClient client;
  private final Properties config = new Properties();
  private final ExecutorService executor
    = Executors.newFixedThreadPool(5);
  private int batchSize;

  protected void parseCommandLine(String args[])
    throws ParseException {

    setClient(config);
    Options opts = new Options();

    Option opt = new Option("r", "remote", true,
      "Remote host to connect " +
        "to");
    opt.setRequired(true);
    opts.addOption(opt);

    opt = new Option("p", "port", true, "Port to connect to");
    opt.setRequired(true);
    opts.addOption(opt);

    opt = new Option("h", "help", false, "Display help");
    opt.setRequired(false);
    opts.addOption(opt);

    opt = new Option("b", "batchSize", true, "Batch Size to use");
    opt.setRequired(false);
    opts.addOption(opt);

    opt = new Option("c", "compression", false, "If set, " +
      "data is compressed before sending");
    opt.setRequired(false);
    opts.addOption(opt);

    opt = new Option("l", "compression-level", false,
      "The compression level " +
        "to use if compression is enabled");
    opt.setRequired(false);
    opts.addOption(opt);

    opt = new Option("s", "ssl", false,
      "If set, ssl is enabled using the " +
        "default Java trust store");
    opt.setRequired(false);
    opts.addOption(opt);

    opt = new Option("i", "maxIoWorkers", true,
      "Set the maximum number of " +
        "worker threads to use for network IO");
    opt.setRequired(false);
    opts.addOption(opt);

    opt = new Option("o", "backoff", false,
      "Backoff failed clients?");
    opt.setRequired(false);
    opts.addOption(opt);

    Parser parser = new GnuParser();
    CommandLine commandLine = parser.parse(opts, args);

    if (commandLine.hasOption("h")) {
      new HelpFormatter().printHelp("UsingFlumeDefaultRPCApp", opts,
        true);
      return;
    }

    parseHostsAndPort(commandLine, config);

    if (commandLine.hasOption("b")) {
      String batchSizeStr = commandLine.getOptionValue("b", "100");
      config.setProperty(CONFIG_BATCH_SIZE, batchSizeStr);
      batchSize = Integer.parseInt(batchSizeStr);

    }

    if (commandLine.hasOption("c")) {
      config.setProperty(CONFIG_COMPRESSION_TYPE,
        commandLine.getOptionValue("compression"));
      if (commandLine.hasOption("l")) {
        config.setProperty(CONFIG_COMPRESSION_LEVEL,
          commandLine.getOptionValue("l"));
      }
    }

    if (commandLine.hasOption("s")) {
      config.getProperty(CONFIG_SSL, commandLine.getOptionValue("s"));
    }

    if (commandLine.hasOption("i")) {
      config.setProperty(MAX_IO_WORKERS,
        commandLine.getOptionValue("i"));
    }

    backoffConfig(commandLine, config);
  }

  protected abstract void setClient(Properties p);

  protected abstract void parseHostsAndPort(CommandLine commandLine,
    Properties config);

  protected abstract void backoffConfig(CommandLine commandLine,
    Properties config);

  @VisibleForTesting
  protected void run(String[] args) throws ParseException {
    parseCommandLine(args);

    final UsingFlumeRPCApp app = this;

    for (int i = 0; i < 5; i++) {
      executor.submit(new Runnable() {
        @Override
        public void run() {
          while (true) {
            app.generateAndSend();
          }
        }
      });
    }

    // Set a shutdown hook to shutdown all the threads and the
    // executor itself
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        executor.shutdown();
        try {
          if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
            executor.shutdownNow();
          }
        } catch (InterruptedException e) {
          LOGGER.warn(
            "Interrupted while attempting to shutdown executor. " +
              "Force terminating the executor now.", e);
          executor.shutdownNow();
        }
        app.closeClient();
      }
    }));

  }

  private synchronized void reconnectIfRequired() {
    if (client != null && !client.isActive()) {
      closeClient();
    }
    // If client is null, it was either never created or was closed by
    // closeClient above
    if (client == null) {
      client = RpcClientFactory.getInstance(config);
    }
  }

  protected synchronized void closeClient() {
    client.close();
    client = null;
  }

  protected void generateAndSend() {
    reconnectIfRequired();
    List<Event> events = new ArrayList<Event>(100);
    for (int i = 0; i < batchSize; i++) {
      events.add(EventBuilder.withBody(
        RandomStringUtils.randomAlphanumeric(1024).getBytes()));
    }
    try {
      client.appendBatch(events);
    } catch (Throwable e) {
      LOGGER.error(
        "Error while attempting to write data to remote host at " +
          "%s:%s. Events will be dropped!");
      // The client cannot be reused, since we don't know why the
      // connection
      // failed. Destroy this client and create a new one.
      reconnectIfRequired();
    }

  }
}
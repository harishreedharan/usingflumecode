package usingflume.ch03;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces
  .IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces
  .IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces
  .IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker
  .InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker
  .KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractEventDrivenSource;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class KinesisSource extends AbstractEventDrivenSource {

  private String streamName;
  private String kinesisEndPoint;
  private KinesisClientLibConfiguration clientConfig;
  private AWSCredentialsProvider credentialsProvider;
  private String workerId;
  private String appName;
  private Worker worker;
  private int batchSize;
  private final IRecordProcessorFactory processorFactory = new
    FlumeRecordProcessorFactory();

  private final ExecutorService executor = Executors
    .newSingleThreadExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setDaemon(true);
        t.setName("Kinesis Source worker thread");
        return t;
      }
    });


  @Override
  protected void doConfigure(Context context) throws FlumeException {
    streamName = context.getString("streamName");
    Preconditions.checkNotNull(streamName,
      "Stream name is required!");
    kinesisEndPoint = context.getString("endPoint");
    Preconditions.checkNotNull(kinesisEndPoint,
      "Kinesis Endpoint must be specified");
    batchSize = context.getInteger("batchSize", 1000);
  }

  @Override
  protected void doStart() throws FlumeException {
    try {
      workerId = InetAddress.getLocalHost().getCanonicalHostName() +
        ": " + getName();
      appName = "Flume Kinesis Source: " + workerId;
      credentialsProvider = new InstanceProfileCredentialsProvider();
      // Needed to verify that we can access credentials.
      credentialsProvider.getCredentials();
    } catch (UnknownHostException e) {
      throw new FlumeException("Error while finding host", e);
    } catch (AmazonClientException e) {
      credentialsProvider = new ProfileCredentialsProvider();
      credentialsProvider.getCredentials();
    }
    clientConfig = new KinesisClientLibConfiguration(appName,
      streamName, credentialsProvider, workerId)
      .withInitialPositionInStream(
        InitialPositionInStream.TRIM_HORIZON)
      .withMaxRecords(batchSize); // Maximum batch size.

    executor.execute(new Runnable() {
      @Override
      public void run() {
        while (!Thread.currentThread().isInterrupted()){
          worker = new Worker(processorFactory, clientConfig);
          worker.run(); // Returns when worker is shutdown
        }
      }
    });
  }

  @Override
  protected void doStop() throws FlumeException {
    worker.shutdown();
    executor.shutdownNow();
  }

  private class FlumeRecordProcessorFactory implements
    IRecordProcessorFactory{

    @Override
    public IRecordProcessor createProcessor() {
      return new FlumeRecordProcessor();
    }
  }

  private class FlumeRecordProcessor implements IRecordProcessor {

    @Override
    public void initialize(String shardId) {
      //No-op
    }

    @Override
    public void processRecords(List<Record> records,
      IRecordProcessorCheckpointer checkpointer) {
      final List<Event> events = new ArrayList<Event>(records.size());
      for (Record record: records) {
        events.add(EventBuilder.withBody(record.getData().array()));
      }
      try {
        getChannelProcessor().processEventBatch(events);
        checkpoint(checkpointer);
      } catch (Exception ex) {
        // Shutdown this worker, so we can restart from the last
        // committed position.
        this.shutdown(checkpointer, null);
      }
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer checkpointer,
      ShutdownReason reason) {
      checkpoint(checkpointer);
    }

    private void checkpoint(IRecordProcessorCheckpointer
      checkpointer) {
      try {
        checkpointer.checkpoint();
      } catch (Exception e) {
        Throwables.propagate(e);
      }
    }
  }
}

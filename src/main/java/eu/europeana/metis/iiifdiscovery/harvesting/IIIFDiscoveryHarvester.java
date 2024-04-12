package eu.europeana.metis.iiifdiscovery.harvesting;

import eu.europeana.metis.harvesting.FullRecord;
import eu.europeana.metis.harvesting.FullRecordHarvestingIterator;
import eu.europeana.metis.harvesting.HarvesterException;
import eu.europeana.metis.harvesting.HarvestingIterator;
import eu.europeana.metis.harvesting.ReportingIteration;
import eu.europeana.metis.harvesting.ReportingIteration.IterationResult;
import europeana.rnd.iiif.discovery.ActivityHandler;
import europeana.rnd.iiif.discovery.JavaNetHttpClient;
import europeana.rnd.iiif.discovery.ProcesssingAlgorithm;
import europeana.rnd.iiif.discovery.SeeAlsoHarvester;
import europeana.rnd.iiif.discovery.ValidationException;
import europeana.rnd.iiif.discovery.model.Activity;
import europeana.rnd.iiif.discovery.model.ActivityType;
import europeana.rnd.iiif.discovery.model.SeeAlso;
import europeana.rnd.iiif.discovery.model.SeeAlsoProfile;
import europeana.rnd.iiif.discovery.model.UpdatedResource;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class IIIFDiscoveryHarvester {

  private final JavaNetHttpClient httpClient = new JavaNetHttpClient();
  private final SeeAlsoHarvester harvester = new SeeAlsoHarvester(httpClient, SeeAlsoProfile.EDM);

  public HarvestingIterator<IIIFRecordStub, IIIFRecordStub> harvestHeaders(String streamUrl)
      throws HarvesterException {
    return new IIIFHeaderIterator(harvester, harvestStream(streamUrl));
  }

  public FullRecordHarvestingIterator<IIIFRecord, IIIFRecordStub> harvestRecords(String streamUrl)
      throws HarvesterException {
    return new IIIFRecordIterator(harvester, harvestStream(streamUrl));
  }

  public String harvestRecord(String recordId) throws IOException {
    return harvestRecord(harvester, recordId);
  }

  private static String harvestRecord(SeeAlsoHarvester harvester, String recordId)
      throws IOException {
    final List<SeeAlso> records;
    try {
      records = harvester.harvestFrom(recordId);
    } catch (ValidationException e) {
      throw new IOException(e);
    }
    if (records.size() == 1) {
      return records.get(0).getMetadataContent();
    } else if (records.size() > 1) {
      throw new IOException("Found multiple records per manifest.");
    }
    return null;
  }

  private List<IIIFRecordStub> harvestStream(String streamUrl) throws HarvesterException {
    final IIIFStreamHandler handler = new IIIFStreamHandler();
    final ProcesssingAlgorithm iiifDiscovery = new ProcesssingAlgorithm(handler, httpClient);
    try {
      iiifDiscovery.processStream(streamUrl);
    } catch (Exception e) {
      throw new HarvesterException(e.getMessage(), e);
    }
    return handler.getRecords();
  }

  private static class IIIFRecordIterator extends IIIFIterator<IIIFRecord> implements
      FullRecordHarvestingIterator<IIIFRecord, IIIFRecordStub> {

    public IIIFRecordIterator(SeeAlsoHarvester harvester, List<IIIFRecordStub> records) {
      super(harvester, records);
    }

    @Override
    public void forEachFiltered(ReportingIteration<IIIFRecord> reportingIteration,
        Predicate<IIIFRecordStub> predicate) throws HarvesterException {
      forEachRecordFiltered(reportingIteration, predicate);
    }
  }

  private static class IIIFHeaderIterator extends IIIFIterator<IIIFRecordStub> {

    public IIIFHeaderIterator(SeeAlsoHarvester harvester, List<IIIFRecordStub> records) {
      super(harvester, records);
    }

    @Override
    public void forEachFiltered(ReportingIteration<IIIFRecordStub> reportingIteration,
        Predicate<IIIFRecordStub> predicate) throws HarvesterException {
      forEachHeaderFiltered(reportingIteration, predicate);
    }
  }

  private abstract static class IIIFIterator<T> implements HarvestingIterator<T, IIIFRecordStub> {

    private final SeeAlsoHarvester harvester;

    private final List<IIIFRecordStub> records;

    public IIIFIterator(SeeAlsoHarvester harvester, List<IIIFRecordStub> records) {
      this.harvester = harvester;
      this.records = records;
    }

    public void forEachRecordFiltered(ReportingIteration<IIIFRecord> reportingIteration,
        Predicate<IIIFRecordStub> predicate) throws HarvesterException {
      forEachHeaderFiltered(recordStub -> {
        final String content = harvestRecord(harvester, recordStub.id());
        if (content != null) {
          return reportingIteration.process(new IIIFRecord(recordStub, content));
        }
        return IterationResult.CONTINUE;
      }, predicate);
    }

    public void forEachHeaderFiltered(ReportingIteration<IIIFRecordStub> reportingIteration,
        Predicate<IIIFRecordStub> predicate) throws HarvesterException {
      for (IIIFRecordStub record : records) {
        if (predicate.test(record)) {
          try {
            final IterationResult result = reportingIteration.process(record);
            if (result == IterationResult.TERMINATE) {
              break;
            }
          } catch (IOException e) {
            throw new HarvesterException("Problem while processing: " + record.id(), e);
          }
        }
      }
    }

    @Override
    public void forEachNonDeleted(ReportingIteration<T> reportingIteration)
        throws HarvesterException {
      forEachFiltered(reportingIteration, record -> !record.deleted);
    }

    @Override
    public Integer countRecords() {
      return records.size();
    }

    @Override
    public void close() {
      // Nothing to do.
    }
  }

  private static class IIIFStreamHandler implements ActivityHandler {

    private final List<IIIFRecordStub> records = new ArrayList<>();

    private Instant lastCrawlTimestamp = null;

    @Override
    public boolean isSupportedResourceType(String type) {
      return type.equals("Manifest");
    }

    @Override
    public void processActivity(Activity activity) {
      try {
        final boolean deleted = switch (activity.getTypeOfActivity()) {
          case Add, Create, Update, Move -> false;
          case Delete, Remove -> true;
          case Refresh -> throw new IllegalStateException(
              "A \"Refresh\" activity should have been handled earlier");
        };
        final String recordId = (activity.getTypeOfActivity() == ActivityType.Move) ?
            new UpdatedResource(activity.getTargetJson()).getId() : activity.getObject().getId();
        records.add(new IIIFRecordStub(recordId, activity.getEndTime(), deleted));
      } catch (ValidationException e) {
        throw new IllegalStateException("Activity should have been validated earlier", e);
      }
    }

    @Override
    public void log(String message) {
      // Nothing to do.
    }

    @Override
    public void log(String message, Exception ex) {
      // Nothing to do.
    }

    @Override
    public Instant getLastCrawlTimestamp(String streamUri) {
      return lastCrawlTimestamp;
    }

    @Override
    public void crawlStart(String streamUri) {
      // Nothing to do.
    }

    @Override
    public void crawlEnd(Instant latestTimestamp) {
      lastCrawlTimestamp = latestTimestamp;
    }

    @Override
    public void crawlFail(String errorMessage, Exception cause) {
      // Nothing to do.
    }

    public List<IIIFRecordStub> getRecords() {
      return records;
    }
  }

  public record IIIFRecordStub(String id, Instant timestamp, boolean deleted) {

  }

  public record IIIFRecord(IIIFRecordStub stub, String content) implements FullRecord {

    @Override
    public void writeContent(OutputStream outputStream) throws IOException {
      if (isDeleted()) {
        throw new IllegalStateException("Record is deleted at source.");
      }
      outputStream.write(content.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public InputStream getContent() {
      if (isDeleted()) {
        throw new IllegalStateException("Record is deleted at source.");
      }
      return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public boolean isDeleted() {
      return stub.deleted();
    }

    @Override
    public String getHarvestingIdentifier() {
      return stub.id();
    }

    @Override
    public Instant getTimeStamp() {
      return stub.timestamp();
    }
  }
}

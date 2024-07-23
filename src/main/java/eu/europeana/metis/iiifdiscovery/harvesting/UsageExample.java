package eu.europeana.metis.iiifdiscovery.harvesting;

import eu.europeana.metis.harvesting.FullRecordHarvestingIterator;
import eu.europeana.metis.harvesting.HarvesterException;
import eu.europeana.metis.harvesting.ReportingIteration.IterationResult;
import eu.europeana.metis.iiifdiscovery.harvesting.IIIFDiscoveryHarvester.IIIFRecord;
import eu.europeana.metis.iiifdiscovery.harvesting.IIIFDiscoveryHarvester.IIIFRecordStub;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public class UsageExample {

  public static void main(String[] args) throws IOException, HarvesterException {

    final String streamUri = "https://iiif-staging.aipberoun.cz/apis/iiif-api/discovery/ordered-collection";
    final Path downloadFile = Files.createTempFile("edm-dump-download", ".zip");
    final AtomicInteger counter = new AtomicInteger(0);
    try (final ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(downloadFile));
        final FullRecordHarvestingIterator<IIIFRecord, IIIFRecordStub> harvestingIterator =
            new IIIFDiscoveryHarvester().harvestRecords(streamUri)) {
      System.out.println("Harvesting " + harvestingIterator.countRecords() + " records.");
      harvestingIterator.forEachNonDeleted(record -> {
        try {

          // THE ENDPOINT ENFORCES A 2 SECOND INTERVAL BETWEEN TWO SUCCESSIVE REQUESTS.
          System.out.println(counter.incrementAndGet() + ": " + record.getHarvestingIdentifier());
          Thread.sleep(2500);

          final String fileName = URLEncoder.encode(record.getHarvestingIdentifier(), StandardCharsets.UTF_8);
          zos.putNextEntry(new ZipEntry(fileName + ".rdf"));
          record.writeContent(zos);
          zos.closeEntry();
        } catch (IOException e) {
          throw new IOException("Could not add to zip file.", e);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        return IterationResult.CONTINUE;
      });
      zos.flush();
    }
    System.out.println("EDM dump file downloaded to :" + downloadFile);
  }
}

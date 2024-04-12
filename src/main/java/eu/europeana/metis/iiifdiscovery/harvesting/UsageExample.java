package eu.europeana.metis.iiifdiscovery.harvesting;

import eu.europeana.metis.harvesting.FullRecordHarvestingIterator;
import eu.europeana.metis.harvesting.HarvesterException;
import eu.europeana.metis.harvesting.ReportingIteration.IterationResult;
import eu.europeana.metis.iiifdiscovery.harvesting.IIIFDiscoveryHarvester.IIIFRecord;
import eu.europeana.metis.iiifdiscovery.harvesting.IIIFDiscoveryHarvester.IIIFRecordStub;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class UsageExample {

  public static void main(String[] args) throws IOException, HarvesterException {
    final String streamUri = "https://iiif-staging.aipberoun.cz/apis/iiif-api/discovery/ordered-collection";
    final Path downloadFile = Files.createTempFile("edm-dump-download", ".zip");
    try (final ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(downloadFile));
        final FullRecordHarvestingIterator<IIIFRecord, IIIFRecordStub> harvestingIterator =
            new IIIFDiscoveryHarvester().harvestRecords(streamUri)) {
      System.out.println("Harvesting " + harvestingIterator.countRecords() + " records.");
      harvestingIterator.forEachNonDeleted(record -> {
        try {
          final String fileName = URLEncoder.encode(record.getHarvestingIdentifier());
          zos.putNextEntry(new ZipEntry(fileName + ".rdf"));
          record.writeContent(zos);
          zos.closeEntry();
        } catch (IOException e) {
          throw new IOException("Could not add to zip file.", e);
        }
        return IterationResult.CONTINUE;
      });
      zos.flush();
    }
    System.out.println("EDM dump file downloaded to :" + downloadFile);
  }
}

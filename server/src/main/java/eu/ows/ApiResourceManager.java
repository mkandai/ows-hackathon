package eu.ows;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.FSDirectory;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton class for managing resources that are used by the application
 * For compatibility of different document types in Lucene indexes, there
 * are currently two types of maps for metadata.
 */
public class ApiResourceManager {

    private static Logger LOGGER = LoggerFactory.getLogger(ApiResourceManager.class);

    private static ApiResourceManager INSTANCE;

    private static Map<String, FSDirectory> indexes;
    private static Map<String, List<SimpleGroup>> metadataGroups;  // used for Lucene indexes containing URLs
    private static Map<String, Map<String, SimpleGroup>> metadata; // used for Lucene indexes containing UUIDs

    private ApiResourceManager() {
        readLuceneIndexes();
        readParquetFiles();
    }

    public static ApiResourceManager getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ApiResourceManager();
        }

        return INSTANCE;
    }
    
    /**
     * Adds all available indexes from the lucene directory to a map
     */
    private void readLuceneIndexes() {
        LOGGER.info("Reading lucene indexes in {}", ApiUtils.getIndexDirPath());
        indexes = new HashMap<String, FSDirectory>();
        File[] indexDirs = new File(ApiUtils.getIndexDirPath()).listFiles();
        for (File indexDir : indexDirs) {
            if (indexDir.getName() != null && indexDir.isDirectory()) {
                String indexName = indexDir.getName();
                LOGGER.info("Adding {} to indexes map", indexName);
                try {
                    indexes.put(indexName, FSDirectory.open(Paths.get(ApiUtils.getIndexDirPath() + indexName)));
                } catch (IOException e) {
                    LOGGER.warn("Could not add {} to indexes map", indexName);
                }
            }
        }
    }

    /**
     * Adds all available Parquet files from the parquet directory to a map
     */
    private void readParquetFiles() {
        LOGGER.info("Reading parquet files {}", ApiUtils.getParquetDirPath());
        metadataGroups = new HashMap<String, List<SimpleGroup>>();
        metadata = new HashMap<String, Map<String, SimpleGroup>>();
        File[] parquetFiles = new File(ApiUtils.getParquetDirPath()).listFiles();
        for (File parquetFile : parquetFiles) {
            if (parquetFile.getName() != null && parquetFile.isFile() &&
               (parquetFile.getName().endsWith(".parquet.gz") || parquetFile.getName().endsWith(".parquet"))) {
                String parquetFilename = parquetFile.getName();
                LOGGER.info("Adding {} to metadata map", parquetFilename);

                List<SimpleGroup> simpleGroups = new ArrayList<>();
                ParquetFileReader reader;
                Configuration conf = new Configuration();
                try {
                    reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(ApiUtils.getParquetDirPath() + parquetFilename), conf));
                    MessageType schema = reader.getFooter().getFileMetaData().getSchema();
                    PageReadStore pages;
                    Map<String, SimpleGroup> metadataPerIndex = new HashMap<String, SimpleGroup>();
                    while ((pages = reader.readNextRowGroup()) != null) {
                        long rows = pages.getRowCount();
                        MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                        RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

                        for (int i = 0; i < rows; i++) {
                            SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
                            simpleGroups.add(simpleGroup);

                            if (schema.containsField("record_id")) {
                                metadataPerIndex.put(simpleGroup.getString("record_id", 0), simpleGroup);
                            } else {
                                metadataPerIndex.put(simpleGroup.getString("id", 0), simpleGroup);
                            }
                        }
                    }
                    metadataGroups.put(parquetFilename.substring(0, parquetFilename.lastIndexOf(".parquet")), simpleGroups);
                    metadata.put(parquetFilename.substring(0, parquetFilename.lastIndexOf(".parquet")), metadataPerIndex);
                    reader.close();
                } catch (IllegalArgumentException | IOException e) {
                    LOGGER.warn("Could not add parquet file {} to metadata map", parquetFilename);
                }
            }
        }
    }

    public Map<String, FSDirectory> getIndexes() {
        return indexes;
    }

    public Map<String, Map<String, SimpleGroup>> getMetadata() {
        return metadata;
    }

    public Map<String, List<SimpleGroup>> getMetadataGroups() {
        return metadataGroups;
    }
}

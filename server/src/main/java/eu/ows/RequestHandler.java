package eu.ows;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Paths;
import java.util.*;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import com.google.gson.Gson;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.MediaType;

import io.javalin.http.Context;

/**
 * Handles GET requests and returns responses.
 */
public class RequestHandler {

    private static Logger LOGGER = LoggerFactory.getLogger(RequestHandler.class);

    private Map<String, FSDirectory> indexes;
    private Map<String, List<SimpleGroup>> metadataGroups;
    private Map<String, Map<String, SimpleGroup>> metadata;

    private final OkHttpClient client = new OkHttpClient();
    public static final MediaType JSON = MediaType.get("application/json");

    private Map<String, float[]> embeddings = new HashMap<>();
    
    public RequestHandler() {
        ApiResourceManager apiResourceManager = ApiResourceManager.getInstance();
        indexes = apiResourceManager.getIndexes();
        metadataGroups = apiResourceManager.getMetadataGroups();
        metadata = apiResourceManager.getMetadata();
    }

    /**
     * Handles a particular HTTP GET request for the endpoint /search.
     * @param ctx Context object required to handle HTTP request
     * @param defaultIndex Name of the index to be used by default if no other index is specified in the request
     * @throws Exception
     */
    public void handleSearchRequest(Context ctx, String defaultIndex) throws Exception {

        /************************************************************
         * MARKER-PARAMETERS                                        *
         * Here the parameters of each search request are handled.  *
         * Currently, only query is a required parameter. You can   *
         * modify existing or add new parameters.                   *
         ************************************************************/

        Query query = createQuery(ctx.queryParam("q"));
        String index = ctx.queryParamAsClass("index", String.class).getOrDefault(defaultIndex);
        String lang = ctx.queryParam("lang");
        String ranking = ctx.queryParam("ranking");
        int numHitsLimit = ctx.queryParamAsClass("limit", Integer.class).getOrDefault(ApiUtils.DEFAULT_RESULTS_LIMIT);
        LOGGER.info("Query: {}", query.toString("contents"));
        LOGGER.info("Index: {}", index);
        LOGGER.info("Lang: {}", lang);
        LOGGER.info("Ranking: {}", ranking);
        LOGGER.info("Limit: {}", numHitsLimit);

        String indexPath = ApiUtils.getIndexDirPath() + index;
        LOGGER.info("Used index for search request: {}", indexPath);

        if (!indexes.containsKey(index)) {
            ctx.result("The index could not be found").status(404);
            return;
        }

        if (numHitsLimit <= 0) {
            ctx.result("The limit must be a positive value").status(400);
            return;
        }

        // Create index from file system directory
        FSDirectory indexDir = indexes.get(index);

        // Check if the index exists
        boolean indexExists = DirectoryReader.indexExists(indexDir);
        LOGGER.info("Index exists: {}", indexExists);

        // Create index reader and index searcher
        IndexReader reader = DirectoryReader.open(indexDir);
        IndexSearcher searcher = createSearcher(indexPath);
        searcher.setSimilarity(new BM25Similarity());

        // Fetch search result as JSON object
        JsonObject result = fetchResult(reader, searcher, query, index, lang, ranking, numHitsLimit);

        // Return data
        LOGGER.info("Returning results");
        ctx.contentType("application/json");
        ctx.result(result.toString());

        // Close the IndexReader
        reader.close();
    }

    /**
     * Creates a Query object from the query string.
     * @param queryString Query as string
     * @return Parsed query as Query object
     * @throws ParseException
     */
    private Query createQuery(String queryString) throws ParseException {

        /************************************************************
         * MARKER-QUERY-PARSING                                     *
         * Here you can change the parsing of the query. Parsing is *
         * currently done using the StandardAnalyzer.               *
         ************************************************************/

        QueryParser queryParser = new QueryParser("contents", new StandardAnalyzer());
        return queryParser.parse(queryString);
    }

    /**
     * Creates an index searcher from the file path of the index.
     * @param indexPath Path of the Lucene index
     * @return IndexSearcher created from given path
     * @throws IOException
     */
    private IndexSearcher createSearcher(String indexPath) throws IOException {
        FSDirectory dir = FSDirectory.open(Paths.get(indexPath));
        IndexReader reader = DirectoryReader.open(dir);
        return new IndexSearcher(reader);
    }

    /**
     * Fetches the result within an index for a given query.
     * @param reader IndexReader to read documents
     * @param searcher IndexSearcher used for searching
     * @param query Query param
     * @param index Index param
     * @param lang Language filter param
     * @param ranking (Re)-Ranking param
     * @param numHitsLimit Limit param
     * @return JSON object containing the search result
     * @throws IOException
     */
    private JsonObject fetchResult(IndexReader reader, IndexSearcher searcher, Query query, String index, String lang, String ranking, int numHitsLimit) throws IOException {
        JsonObject resultObject = new JsonObject();
        List<JsonObject> fieldObjects = new ArrayList<JsonObject>();
        JsonArray sortedDocsArray = new JsonArray();
        TopDocs topDocs = new TopDocs(null, null);

        boolean isFirstSearchIteration = true;
        boolean metadataExistsForIndex = metadataGroups.containsKey(index);

        // Iteratively increase the number of hits until the hit limit has been reached
        // or no more documents could be found
        while (fieldObjects.size() < numHitsLimit) {

            /************************************************************
             * MARKER-LUCENE-SEARCH                                     *
             * Here the query is searched in the Lucene index. For the  *
             * first search iteration, .search() is used while for the  *
             * subsequent search iterations .searchAfter() is used.     *
             ************************************************************/

            // Perform the search
            if (isFirstSearchIteration) {
                topDocs = searcher.search(query, numHitsLimit);
                isFirstSearchIteration = false;
            } else {
                ScoreDoc lastScoreDoc = topDocs.scoreDocs[topDocs.scoreDocs.length-1];
                LOGGER.info("Last ScoreDoc: {}", lastScoreDoc);
                topDocs = searcher.searchAfter(lastScoreDoc, query, numHitsLimit - fieldObjects.size());
            }

            ScoreDoc[] hits = topDocs.scoreDocs;
            LOGGER.info("Num results: {}", topDocs.totalHits);

            if (topDocs.scoreDocs.length == 0) {
                // No (more) documents found, stop fetching results
                break;
            }

            // Collect the search results
            List<Document> documents = new ArrayList<>();
            for (ScoreDoc hit : hits) {
                int docId = hit.doc;
                Set<String> fieldsToLoad = new HashSet<>();
                fieldsToLoad.add("content");
                Document document = reader.document(docId);
                documents.add(document);
            }

            // Prepare the result object
            SearchResult result = new SearchResult(documents);

            // Convert result to JSON object
            LOGGER.info("Creating JSON object from search result");
            ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
            String resp = ow.writeValueAsString(result);
            JsonObject respObject = new Gson().fromJson(resp, JsonObject.class);
            JsonArray docsArray = new JsonArray();
            for (JsonElement element : respObject.getAsJsonArray("results")) {
                JsonObject docObject = element.getAsJsonObject().get("fields").getAsJsonArray().get(0).getAsJsonObject();
                docObject.add("url", docObject.get("charSequenceValue"));
                docObject.remove("charSequenceValue");
                docsArray.add(docObject);
            }

            /************************************************************
             * MARKER-METADATA-ENRICHMENT                               *
             * Here metadata is added to each relevant document.        *
             * Depending on the index, the enrichment is either done    *
             * via URL or via UUID.                                     *
             ************************************************************/

            // Parquet metadata inclusion
            LOGGER.info("Adding metadata to search result");
            if (metadataExistsForIndex) {
                for (JsonElement element : docsArray) {
                    JsonObject fieldObject = element.getAsJsonObject();

                    // Get the links, to be queried from parquet
                    String luceneValue = fieldObject.get("url").getAsString();

                    // Check if the URL or the UUID is stored in the index
                    if (luceneValue.startsWith("http")) {
                        // Index contains documents identified by URLs
                        List<SimpleGroup> simpleGroups = metadataGroups.get(index).subList(0,4);
                        simpleGroups.sort((d1, d2) -> {
                            if(!embeddings.containsKey(query.toString())) {
                                embeddings.put(query.toString(), computeEmbedding(query.toString()));
                            }
                            float[] embeddingQ = embeddings.get(query.toString());
                            String content1= d1.toString();
                            String content2= d2.toString();

                            if(!embeddings.containsKey(content1)) {
                                embeddings.put(content1, computeEmbedding(content1));
                            }

                            if(!embeddings.containsKey(content2)) {
                                embeddings.put(content2, computeEmbedding(content2));
                            }

                            return cosine_similarity(embeddingQ, embeddings.get(content1), dotProduct(embeddingQ, embeddings.get(content1))).compareTo(
                                    cosine_similarity(embeddingQ, embeddings.get(content2), dotProduct(embeddingQ, embeddings.get(content2))
                                    ));
                        });

                        for (SimpleGroup simpleGroup : simpleGroups) {

                            String parquetUrl = ApiUtils.buildParquetUrl(simpleGroup);

                            if (ApiUtils.isSameUrl(luceneValue, parquetUrl) && ApiUtils.isInLanguage(simpleGroup, lang)) {
                                JsonObject updatedFieldObject = ApiUtils.updateFieldObject(fieldObject, simpleGroup);
                                fieldObjects.add(updatedFieldObject);
                            }
                        }
                    } else {
                        // Index contains documents identified by UUIDs
                        SimpleGroup simpleGroup = metadata.get(index).get(luceneValue);
                        if (ApiUtils.isInLanguage(simpleGroup, lang)) {
                            JsonObject updatedFieldObject = ApiUtils.updateFieldObject(fieldObject, simpleGroup, true);
                            fieldObjects.add(updatedFieldObject);
                        }
                    }
                }

            } else {
                LOGGER.info("Could not find a parquet file with metadata for index {}", index);
                sortedDocsArray = docsArray;
            }
        }

        // Optionally re-rank the search results
        if (metadataExistsForIndex) {
            LOGGER.info("Re-ranking result with key: {}", ranking);
            sortedDocsArray = ApiUtils.reRankResults(fieldObjects, ranking);
        }

        // Add array with crawled text to Json object
        resultObject.add("results", sortedDocsArray);

        return resultObject;
    }

    public Double cosine_similarity(float[] input1_vector, float[] input2_vector, double dot_product)  {
        double norm_a = 0.0;
        double norm_b = 0.0;
        //Its assumed input1_vector and input2_vector have same length (300 dimensions)
        for (int i = 0; i < input1_vector.length; i++)
        {
            norm_a += Math.pow(input1_vector[i], 2);
            norm_b += Math.pow(input2_vector[i], 2);
        }

        return (dot_product / (Math.sqrt(norm_a) * Math.sqrt(norm_b)));
    }

    public Float dotProduct(float[] a, float[] b) {
        float dotProduct = 0;
        for (int i = 0; i < a.length; i++)
            dotProduct += a[i] * b[i];

        return dotProduct;
    }

    public float[] computeEmbedding(String content) {
        Map<String, String> map = new HashMap<>();
        map.put("sentence", content);
        Gson gson = new Gson();
        RequestBody body = RequestBody.create(gson.toJson(map), JSON);
        String url = "http://127.0.0.1:5000/embed";
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();
        try (Response response = client.newCall(request).execute()) {
            EmbeddingResponse embeddingResponse = gson.fromJson(response.body().string(), EmbeddingResponse.class);
            return embeddingResponse.embeddings[0];
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new float[]{0f};
    }

    private static class EmbeddingResponse {
        float[][] embeddings;
    }
}

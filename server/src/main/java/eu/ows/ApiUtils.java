package eu.ows;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * Provides constants and helper methods with regards to the API.
 */
public abstract class ApiUtils {
    private static Logger LOGGER = LoggerFactory.getLogger(ApiUtils.class);

    public static final String RANKING_ASC = "asc";
    public static final String RANKING_DESC = "desc";
    public static final int DEFAULT_RESULTS_LIMIT = 20;

    public static final String DEFAULT_INDEX_DIR_PATH = "../resources/lucene/";
    public static final String DEFAULT_PARQUET_DIR_PATH = "../resources/parquet/";

    private static String indexDirPath;
    private static String parquetDirPath;

    private static SimpleDateFormat warcDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    /**
     * Getter method for the index directory path.
     * @return Path of directory where indexes are stored
     */
    public static String getIndexDirPath() {
        return indexDirPath;
    }

    /**
     * Setter method for the index directory path.
     * @param newIndexDirPath New path of index directory
     */
    public static void setIndexDirPath(String newIndexDirPath) {
        indexDirPath = newIndexDirPath;
    }

    /**
     * Getter method for the parquet directory path.
     * @return Path of directory where metadata as parquet files are stored
     */
    public static String getParquetDirPath() {
        return parquetDirPath;
    }

    /**
     * Setter method for the parquet directory path.
     * @param newParquetDirPath New path of parquet directory
     */
    public static void setParquetDirPath(String newParquetDirPath) {
        parquetDirPath = newParquetDirPath;
    }

    /**
     * Builds the full URL out of the fields from the Parquet file of a single record.
     * Reference: https://datatracker.ietf.org/doc/html/rfc3986#section-5.2.2
     * @param simpleGroup Record in Parquet file
     * @return Full URL of a Parquet record composed of its fields
     */
    public static String buildParquetUrl(SimpleGroup simpleGroup) {

        String parquetUrl = "";

        if (simpleGroup.getType().containsField("record_id")) {
            parquetUrl = simpleGroup.getString("url", 0);

            // Optionally extract URL between < and > that was added by crawler
            if (parquetUrl.startsWith("<")) {
                parquetUrl = parquetUrl.substring(1);
            }
            if (parquetUrl.endsWith(">")) {
                parquetUrl = parquetUrl.substring(0, parquetUrl.length() - 1);
            }

        } else {
            parquetUrl = simpleGroup.getString("url_scheme", 0) + "://";
        
            if (fieldIsNotEmpty(simpleGroup, "url_subdomain")) {
                parquetUrl += simpleGroup.getString("url_subdomain", 0) + ".";
            }

            parquetUrl += simpleGroup.getString("url_domain", 0) + "." + simpleGroup.getString("url_suffix", 0);

            if (fieldIsNotEmpty(simpleGroup, "url_path")) {
                parquetUrl += simpleGroup.getString("url_path", 0);
            }

            if (fieldIsNotEmpty(simpleGroup, "url_query")) {
                parquetUrl += "?" + simpleGroup.getString("url_query", 0);
            }

            if (fieldIsNotEmpty(simpleGroup, "url_fragment")) {
                parquetUrl += "#" + simpleGroup.getString("url_fragment", 0);
            }
        }

        return parquetUrl;
    }

    /**
     * Checks if a given field of a record in the Parquet file is empty or not.
     * @param simpleGroup Record in Parquet file
     * @param field Name of field in Parquet file
     * @return True if the given field in the record is not empty, else false
     */
    public static boolean fieldIsNotEmpty(SimpleGroup simpleGroup, String field) {
        return simpleGroup.getString(field, 0) != null && !simpleGroup.getString(field, 0).isEmpty();
    }

    /**
     * Compares the URL from a document in the Lucene index with the URL of a record in the Parquet file.
     * @param luceneUrl URL of a document stored in the Lucene index
     * @param parquetUrl URL stored in the Parquet file
     * @return True if the URLs are equal, else false
     */
    public static boolean isSameUrl(String luceneUrl, String parquetUrl) {
        return luceneUrl.equals(parquetUrl) || luceneUrl.equals(parquetUrl + "/") || parquetUrl.equals(luceneUrl + "/");
    }

    /**
     * Checks if a record in the Parquet file is in the given language.
     * @param simpleGroup Record in Parquet file
     * @param lang Language to be compared with
     * @return True if the language of the record equals the given language, else false
     */
    public static boolean isInLanguage(SimpleGroup simpleGroup, String lang) {

        /************************************************************
         * MARKER-LANGUAGE-FILTER                                   *
         * Here the comparison between a relevant document and the  *
         * the selected language filter is done. Also check         *
         * RequestHandler where this method is called.              *
         ************************************************************/

        return lang == null || simpleGroup.getString("language", 0).equalsIgnoreCase(lang);
    }

    /**
     * Updates several values of a JsonObject based on a record of the Parquet file.
     * @param fieldObject JsonObject to be updated
     * @param simpleGroup Record in Parquet file
     * @return A deep updated copy of the passed JsonObject
     */
    public static JsonObject updateFieldObject(JsonObject fieldObject, SimpleGroup simpleGroup) {
        return updateFieldObject(fieldObject, simpleGroup, false);
    }

    /**
     * Updates several values of a JsonObject based on a record of the Parquet file.
     * @param fieldObject JsonObject to be updated
     * @param simpleGroup Record in Parquet file
     * @param setUrl Flag if URL should be part of the update
     * @return A deep updated copy of the passed JsonObject
     */
    public static JsonObject updateFieldObject(JsonObject fieldObject, SimpleGroup simpleGroup, boolean setUrl) {
        String id = (simpleGroup.getType().containsField("record_id")) ? simpleGroup.getString("record_id", 0) : simpleGroup.getString("id", 0);
        fieldObject.addProperty("id", id);

        String documentTitle = simpleGroup.getString("title", 0);
        fieldObject.addProperty("title", documentTitle.trim());

        String documentText = simpleGroup.getString("plain_text", 0);
        fieldObject.addProperty("textSnippet", longestSequence(documentText).trim());

        String documentLanguage = simpleGroup.getString("language", 0);
        fieldObject.addProperty("language", documentLanguage.trim());

        Object documentWarcDate = null;
        if (simpleGroup.getType().containsField("record_id")) {
            documentWarcDate = simpleGroup.getString("warc_date", 0);
            try {
                documentWarcDate = warcDateFormat.parse((String) documentWarcDate).getTime() * 1000;
            } catch (ParseException e) {
                LOGGER.warn("Exception while parsing date: {}", documentWarcDate);
            }
        } else {
            documentWarcDate = simpleGroup.getLong("warc_date", 0);
        }
        fieldObject.addProperty("warcDate", documentWarcDate.toString());

        int wordCount = simpleGroup.getString("plain_text", 0).split("\\s+").length;
        fieldObject.addProperty("wordCount", wordCount);

        if (setUrl) {
            fieldObject.remove("charSequenceValue");
            fieldObject.addProperty("url", buildParquetUrl(simpleGroup));
        }

        return fieldObject.deepCopy();
    }

    /**
     * Converts a JSON string to a JsonObject.
     * @param jsonString JSON as string
     * @return JsonObject created from JSON string
     */
    public static JsonObject convertToJSON(String jsonString) {
        Gson gson = new Gson();
        return gson.fromJson(jsonString, JsonObject.class);
    }

    /**
     * Searches for the longest sequence of characaters in a text without a linebreak.
     * @param input Full text
     * @return Longest sequence of characters in a string without a linebreak
     */
    public static String longestSequence(String input) {

        /************************************************************
         * MARKER-TEXT-SNIPPET                                      *
         * Here you can change the text snippet returned by the API *
         * which is currently a sequence from the start of the full *
         * text until the first line break in the full text.        *
         ************************************************************/

        String[] sequences = input.split("\n");
        String longestSequence = "";

        for (String sequence : sequences) {
            if (sequence.length() > longestSequence.length()) {
                longestSequence = sequence;
            }
        }

        return longestSequence;
    }

    /**
     * Re-ranks the results of a search by word count in the specified sort order.
     * @param results List of results from a search
     * @param ranking Type of sort order
     * @return Re-ranked results sorted by the specified sort order
     */
    public static JsonArray reRankResults(List<JsonObject> results, String ranking) {
        JsonArray sortedResultsArray = new JsonArray();

        /************************************************************
         * MARKER-RERANKING                                         *
         * Here you can change the re-ranking of search results     *
         * which is currently based on the word count of documents. *
         ************************************************************/

        if (ranking != null && (ranking.equalsIgnoreCase(RANKING_ASC) || ranking.equalsIgnoreCase(RANKING_DESC))) {
            Collections.sort(results, new Comparator<JsonObject>() {

                @Override
                public int compare(JsonObject o1, JsonObject o2) {
                    if (ranking.equalsIgnoreCase(RANKING_DESC)) {
                        return o2.get("wordCount").getAsInt() - o1.get("wordCount").getAsInt();
                    }
                    return o1.get("wordCount").getAsInt() - o2.get("wordCount").getAsInt();
                }

            });
        }

        for (JsonObject element : results) {
            sortedResultsArray.add(element);
        }

        return sortedResultsArray;
    }
}

package eu.ows;

import java.util.List;

import org.apache.lucene.document.Document;

/**
 * Stores all results returned by a single search in a Lucene index.
 */
public class SearchResult {
    private List<Document> results;

    public SearchResult(List<Document> results) {
        this.results = results;
    }

    public List<Document> getResults() {
        return results;
    }
}

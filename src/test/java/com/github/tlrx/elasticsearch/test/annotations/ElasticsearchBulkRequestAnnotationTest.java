package com.github.tlrx.elasticsearch.test.annotations;

import com.github.tlrx.elasticsearch.test.support.junit.runners.ElasticsearchRunner;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * Test class for {@link ElasticsearchBulkRequest} annotation.
 *
 * @author tlrx
 */
@RunWith(ElasticsearchRunner.class)
public class ElasticsearchBulkRequestAnnotationTest {

    @ElasticsearchNode(local = false)
    Node node;

    @ElasticsearchClient
    Client client;

    @Test
    @ElasticsearchIndex(indexName = "documents", forceCreate = true)
    @ElasticsearchBulkRequest(dataFile = "com/github/tlrx/elasticsearch/test/annotations/documents/bulk1.json")
    public void testElasticsearchBulkRequest1() {
        // Count number of documents
        SearchResponse countResponse = client.prepareSearch("documents")
                .setSource(new SearchSourceBuilder().size(0))
                .setTypes("doc1")
                .execute()
                .actionGet();
        assertEquals(6, countResponse.getHits().getTotalHits());
    }

    @Test
    @ElasticsearchIndex(indexName = "documents")
    @ElasticsearchBulkRequest(dataFile = "com/github/tlrx/elasticsearch/test/annotations/documents/bulk2.json")
    public void testElasticsearchBulkRequest2() {
        // Count number of documents
        SearchResponse countResponse = client.prepareSearch("documents")
                .setSource(new SearchSourceBuilder().size(0))
                .setTypes("doc1")
                .execute()
                .actionGet();
        assertEquals(9, countResponse.getHits().getTotalHits());
    }
}

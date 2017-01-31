package com.github.tlrx.elasticsearch.test.annotations;

import com.github.tlrx.elasticsearch.test.support.junit.runners.ElasticsearchRunner;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.node.Node;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Test class for {@link ElasticsearchMapping} and {@link ElasticsearchMappingField} annotations.
 *
 * @author tlrx
 */
@RunWith(ElasticsearchRunner.class)
public class ElasticsearchMappingAnnotationTest {

    @ElasticsearchNode
    Node node;

    @ElasticsearchAdminClient
    AdminClient adminClient;

    @Test
    @SuppressWarnings("unchecked")
    @ElasticsearchIndex(indexName = "library",
            mappings = {
                    @ElasticsearchMapping(typeName = "rating",
                            source = true,
                            parent = "book",
                            properties = {
                                    @ElasticsearchMappingField(name = "stars", index = Index.Not_Analyzed, type = Types.Integer)
                            }
                    ),
                    @ElasticsearchMapping(typeName = "book",
                            source = false,
                            properties = {
                                    @ElasticsearchMappingField(name = "title", store = Store.Yes, type = Types.Text),
                                    @ElasticsearchMappingField(name = "author", store = Store.No, type = Types.Text, index = Index.Not_Analyzed),
                                    @ElasticsearchMappingField(name = "description", store = Store.Yes, type = Types.Text, index = Index.Analyzed, analyzerName = "standard"),
                                    @ElasticsearchMappingField(name = "role", store = Store.No, type = Types.Text, index = Index.Analyzed, analyzerName = "keyword", searchAnalyzerName = "standard"),
                                    @ElasticsearchMappingField(name = "publication_date", store = Store.No, type = Types.Date, index = Index.Not_Analyzed),
                                    @ElasticsearchMappingField(name = "name", type = Types.Text, index = Index.Analyzed, termVector = TermVector.With_Offsets,
                                            fields = {
                                                    @ElasticsearchMappingSubField(name = "untouched", type = Types.Text, index = Index.Not_Analyzed, termVector = TermVector.With_Positions_Offsets),
                                                    @ElasticsearchMappingSubField(name = "stored", type = Types.Text, index = Index.No, store = Store.Yes)
                                            }
                                    )
                            })
            })
    public void testElasticsearchMapping() {

        // Checks if the index has been created
        IndicesExistsResponse existResponse = adminClient.indices()
                .prepareExists("library")
                .execute().actionGet();

        assertTrue("Index must exist", existResponse.isExists());

        // Checks if mapping has been created
        ClusterStateResponse stateResponse = adminClient.cluster()
                .prepareState()
                .setIndices("library").execute()
                .actionGet();

        IndexMetaData indexMetaData = stateResponse.getState().getMetaData().index("library");

        // Test book mapping
        MappingMetaData mappingMetaData = indexMetaData.getMappings().get("book");
        assertNotNull("Mapping must exists", mappingMetaData);

        try {
            Map<String, Object> def = mappingMetaData.sourceAsMap();

            // Check _source
            Map<String, Object> source = (Map<String, Object>) def.get("_source");
            assertNotNull("_source must exists", source);
            assertEquals(Boolean.FALSE, source.get("enabled"));

            // Check properties
            Map<String, Object> properties = (Map<String, Object>) def.get("properties");
            assertNotNull("properties must exists", properties);

            // Check title
            Map<String, Object> title = (Map<String, Object>) properties.get("title");
            assertEquals("text", title.get("type"));
            assertEquals(Boolean.TRUE, title.get("store"));

            // Check author
            Map<String, Object> author = (Map<String, Object>) properties.get("author");
            assertEquals("text", author.get("type"));
            assertNull("Store = No must be null", author.get("store"));

            // Check description
            Map<String, Object> description = (Map<String, Object>) properties.get("description");
            assertNull("index = analyzed must be null", description.get("index"));
            assertEquals("text", description.get("type"));
            assertEquals(Boolean.TRUE, description.get("store"));
            assertEquals("standard", description.get("analyzer"));

            // Check role
            Map<String, Object> role = (Map<String, Object>) properties.get("role");
            assertNull("index = analyzed must be null", role.get("index"));
            assertEquals("text", role.get("type"));
            assertNull("Store = No must be null", role.get("store"));
            assertEquals("keyword", role.get("analyzer"));
            assertEquals("standard", role.get("search_analyzer"));

            // Check name
            Map<String, Object> name = (Map<String, Object>) properties.get("name");
            assertEquals("text", name.get("type"));
            Map<String, Object> fields = (Map<String, Object>) name.get("fields");
            assertNotNull("fields must exists", fields);

            // Check name.untouched
            Map<String, Object> untouched = (Map<String, Object>) fields.get("untouched");
            assertEquals("text", untouched.get("type"));
            assertNull("Store = No must be null", untouched.get("store"));
            assertEquals("with_positions_offsets", untouched.get("term_vector"));

            // Check name.stored
            Map<String, Object> stored = (Map<String, Object>) fields.get("stored");
            assertEquals("text", stored.get("type"));
            assertEquals(Boolean.TRUE, stored.get("store"));
            assertEquals(false, stored.get("index"));

        } catch (IOException e) {
            fail("Exception when reading mapping metadata");
        }

        // Test rating mapping
        mappingMetaData = indexMetaData.getMappings().get("rating");
        assertNotNull("Mapping must exists", mappingMetaData);

        try {
            Map<String, Object> def = mappingMetaData.sourceAsMap();

            // Check _source
            Map<String, Object> source = (Map<String, Object>) def.get("_source");
            assertNull("_source must exists", source);

            // Check _parent
            Map<String, Object> parent = (Map<String, Object>) def.get("_parent");
            assertNotNull("_parent must exists", parent);
            assertEquals("book", parent.get("type"));

            // Check properties
            Map<String, Object> properties = (Map<String, Object>) def.get("properties");
            assertNotNull("properties must exists", properties);

            // Check stars
            Map<String, Object> stars = (Map<String, Object>) properties.get("stars");
            assertEquals("integer", stars.get("type"));

        } catch (IOException e) {
            fail("Exception when reading mapping metadata");
        }
    }

}
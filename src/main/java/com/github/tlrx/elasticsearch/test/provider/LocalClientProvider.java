/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.github.tlrx.elasticsearch.test.provider;


import com.github.tlrx.elasticsearch.test.support.junit.handlers.annotations.ElasticsearchNodeAnnotationHandler;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.Netty4Plugin;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;

import static java.util.Collections.singletonList;

/**
 * LocalClientProvider instantiates a local node with in-memory index store type.
 */
public class LocalClientProvider implements ClientProvider {

    private Node node = null;
    private Client client = null;
    private Settings settings = null;

    public LocalClientProvider() {
    }

    public LocalClientProvider(Settings settings) {
        this.settings = settings;
    }

    @Override
    public void open() {
        if (node == null || node.isClosed()) {
            // Build and start the node
            node = new MyNode(buildNodeSettings(), singletonList(Netty4Plugin.class));
            try {
                node.start();
            } catch (NodeValidationException e) {
                throw new RuntimeException();
            }

            // Get a client
            client = node.client();

            // Wait for Yellow status
            client.admin().cluster()
                    .prepareHealth()
                    .setWaitForYellowStatus()
                    .setTimeout(TimeValue.timeValueMinutes(1))
                    .execute()
                    .actionGet();
        }
    }

    @Override
    public Client client() {
        return client;
    }

    @Override
    public void close() {
        if (client() != null) {
            client.close();
        }

        if ((node != null) && (!node.isClosed())) {
            try {
                node.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            deleteRecursively(new File("./target/elasticsearch-test/"));
        }
    }

    /**
     * Recursively delete a directory.
     * Links are not handled properly.
     */
    public static void deleteRecursively(File dir) {
        try {
            Files.walkFileTree(dir.toPath(), new FileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                    return FileVisitResult.TERMINATE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
        }
    }

    protected Settings buildNodeSettings() {
        // Build settings

        Settings.Builder builder = Settings.builder()
                .put("node.name", "node-test-" + System.currentTimeMillis())
                .put("node.data", true)
                .put("node.attr.local", true)
                .put("node.max_local_storage_nodes", 10)
                .put("cluster.name", "cluster-test-" + getLocalHostName())
                .put("path.home", "./target/elasticsearch-test")
                .put("path.data", "./target/elasticsearch-test/data")
                .put("path.logs", "./target/elasticsearch-test/logs")
                //.put("path.work", "./target/elasticsearch-test/work")
                //.put("index.number_of_shards", "1")
                //.put("index.number_of_replicas", "0")
                //.put("cluster.routing.schedule", "50ms")
                .put("transport.type", "netty4")
                .put("http.type", "netty4")
                .put("http.enabled", "true");

        if (settings != null) {
            builder.put(settings);
        }

        return builder.build();
    }

    /**
     * Get local hostname
     */
    public static String getLocalHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown";
        }
    }

    private static class MyNode extends Node {
        public MyNode(Settings preparedSettings, Collection<Class<? extends Plugin>> classpathPlugins) {
            super(InternalSettingsPreparer.prepareEnvironment(preparedSettings, null), classpathPlugins);
        }
    }
}

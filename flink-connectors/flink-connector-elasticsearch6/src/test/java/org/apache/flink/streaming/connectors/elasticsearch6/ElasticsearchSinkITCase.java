/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.elasticsearch6;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkTestBase;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.testutils.SourceSinkDataTestKit;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.streaming.connectors.elasticsearch.testutils.SourceSinkDataTestKit.DATA_FIELD_NAME;
import static org.apache.flink.streaming.connectors.elasticsearch.testutils.SourceSinkDataTestKit.TYPE_NAME;

/**
 * IT cases for the {@link ElasticsearchSink}.
 */
public class ElasticsearchSinkITCase extends ElasticsearchSinkTestBase {

	@Test
	public void testTransportClient() throws Exception {
/*		final String index = "transport-client-test-index";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<Integer, String>> source = env.addSource(new SourceSinkDataTestKit.TestDataSourceFunction());

		Map<String, String> userConfig = new HashMap<>();
		// This instructs the sink to emit after every element, otherwise they would be buffered
		userConfig.put(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");
		userConfig.put("cluster.name", CLUSTER_NAME);

		source.addSink(createElasticsearchSinkForEmbeddedNode(
			userConfig, new TestElasticsearchSinkFunction(index)));

		env.execute("Elasticsearch TransportClient Test");

		// verify the results
		Client client = embeddedNodeEnv.getClient();
		SourceSinkDataTestKit.verifyProducedSinkData(client, index);

		client.close();*/
		runTransportClientTest();
	}

	@Test
	public void testNullTransportClient() throws Exception {
		runNullTransportClientTest();
	}

	@Test
	public void testEmptyTransportClient() throws Exception {
		runEmptyTransportClientTest();
	}

	@Test
	public void testTransportClientFails() throws Exception {
		runTransportClientFailsTest();
	}

	@Override
	protected <T> ElasticsearchSinkBase<T> createElasticsearchSink(Map<String, String> userConfig,
																   List<InetSocketAddress> transportAddresses,
																   ElasticsearchSinkFunction<T> elasticsearchSinkFunction) {
		return new ElasticsearchSink<>(userConfig, transportAddresses, elasticsearchSinkFunction);
	}

	@Override
	protected <T> ElasticsearchSinkBase<T> createElasticsearchSinkForEmbeddedNode(
		Map<String, String> userConfig, ElasticsearchSinkFunction<T> elasticsearchSinkFunction) throws Exception {

		List<InetSocketAddress> transports = new ArrayList<>();
		transports.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));

		return new ElasticsearchSink<>(userConfig, transports, elasticsearchSinkFunction);
	}

	/**
	 * A {@link ElasticsearchSinkFunction} that indexes each element it receives to a sepecified Elasticsearch index.
	 */
	public static class TestElasticsearchSinkFunction implements ElasticsearchSinkFunction<Tuple2<Integer, String>> {
		private static final long serialVersionUID = 1L;

		private final String index;

		/**
		 * Create the sink function, specifying a target Elasticsearch index.
		 *
		 * @param index Name of the target Elasticsearch index.
		 */
		public TestElasticsearchSinkFunction(String index) {
			this.index = index;
		}

		public IndexRequest createDocWriteRequest(Tuple2<Integer, String> element) {
			Map<String, Object> json = new HashMap<>();
			json.put(DATA_FIELD_NAME, element.f1);
			return new IndexRequest(index, TYPE_NAME, element.f0.toString()).source(json);
		}

		@Override
		public void process(Tuple2<Integer, String> element, RuntimeContext ctx, RequestIndexer indexer) {
			indexer.add(createDocWriteRequest(element));
		}
	}

}

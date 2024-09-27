/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.indexer.bcubefilter;


import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.util.NutchConfiguration;
import org.junit.Test;

public class DiscardBCubeIndexingFilterTest {

	@Test
	public void DiscardBCubeIndexingFilter_wont_discard_xml_documents() throws Exception {
		Configuration conf = NutchConfiguration.create();
		conf.setBoolean("moreIndexingFilter.indexMimeTypeParts", true);
		DiscardBCubeIndexingFilter filter = new DiscardBCubeIndexingFilter();
		filter.setConf(conf);

		NutchDocument doc = new NutchDocument();
		
		doc.add("type", "application/opensearchdescription+xml");
		doc.add("id", "https://notrealurl.example.net");

		Parse parse = mock(Parse.class);
		Metadata metadata = new Metadata();
		ParseData parseData = new ParseData();
		parseData.setParseMeta(metadata);

		// Mock parser response
		when(parse.getData()).thenReturn(parseData);

		filter.filter(doc, parse, null, null, null);
		
		assertNotNull(doc);
		
		assertTrue(doc.getFieldNames().contains("type"));
		NutchField contentTypeField = doc.getField("type");
		
		String typeValue = contentTypeField.getValues().get(0).toString();
		assertTrue(typeValue.equals("application/opensearchdescription+xml"));
	}

	@Test
	public void DiscardBCubeIndexingFilter_will_discard_non_xml_documents() throws Exception {
		Configuration conf = NutchConfiguration.create();
		conf.setBoolean("moreIndexingFilter.indexMimeTypeParts", true);
		DiscardBCubeIndexingFilter filter = new DiscardBCubeIndexingFilter();
		filter.setConf(conf);

		NutchDocument doc = new NutchDocument();
		
		doc.add("type", "text/html");
		doc.add("id", "https://notrealurl.example.net");

		Parse parse = mock(Parse.class);
		Metadata metadata = new Metadata();
		ParseData parseData = new ParseData();
		parseData.setParseMeta(metadata);

		// Mock parser response
		when(parse.getData()).thenReturn(parseData);

		doc = filter.filter(doc, parse, null, null, null);
		
		assertNull(doc);
	}

    	@Test
	public void DiscardBCubeIndexingFilter_will_parse_html_documents() throws Exception {
		Configuration conf = NutchConfiguration.create();
		conf.setBoolean("moreIndexingFilter.indexMimeTypeParts", true);
		conf.set("indexingfilter.bcube.allowed.html.regex", "<pre[^>]*>\\n?(Attributes \\{.*\\})\\n?</pre>");
		conf.set("indexingfilter.bcube.allowed.mimetypes", "text/html");
		DiscardBCubeIndexingFilter filter = new DiscardBCubeIndexingFilter();
		filter.setConf(conf);

		NutchDocument doc = new NutchDocument();
		
		doc.add("type", "text/html");
		doc.add("id", "https://notrealurl.example.net/foobar");
		
		Parse parse = mock(Parse.class);
		Metadata metadata = new Metadata();

		// simulate <pre> section in HTML page
		String content = String.join("\n",
					     "<!DOCTYPE html>",
					     "<html> <pre style=\"white-space:pre-wrap;\">",
					     "Attributes {",
					     "s {",
					     "alkalinity_total {",
					     " Float32 actual_range 0.214, 2.899;",
					     " String ioos_category &quot;Hydrology&quot;;",
					     " String units &quot;millequivalents/liter&quot;;",
					     "}",
					     "</pre>",
					     "</html>");
		
		metadata.add("raw_content", content);
		ParseData parseData = new ParseData();
		parseData.setParseMeta(metadata);

		// Mock parser response
		when(parse.getData()).thenReturn(parseData);

		doc = filter.filter(doc, parse, null, null, null);

		assertNotNull(doc);

		assertTrue(doc.getFieldNames().contains("type"));
		NutchField contentTypeField = doc.getField("type");
		
		String typeValue = contentTypeField.getValues().get(0).toString();
		assertTrue(typeValue.equals("text/html"));
		
	}

    	@Test
	public void DiscardBCubeIndexingFilter_skip_indexing_url_filters() throws Exception {
		Configuration conf = NutchConfiguration.create();
		conf.setBoolean("moreIndexingFilter.indexMimeTypeParts", true);
		conf.set("indexingfilter.bcube.allowed.mimetypes", "text/html text/xml");		
		conf.set("indexingfilter.bcube.forbidden.url.patterns", "wiki");
		DiscardBCubeIndexingFilter filter = new DiscardBCubeIndexingFilter();
		filter.setConf(conf);

		NutchDocument doc = new NutchDocument();
		doc.add("type", "text/html");
		doc.add("id", "https://en.wikipedia.org/wiki/Apache_Nutch");
		
		Parse parse = mock(Parse.class);
		Metadata metadata = new Metadata();
		ParseData parseData = new ParseData();
		parseData.setParseMeta(metadata);

		// Mock parser response
		when(parse.getData()).thenReturn(parseData);
		doc = filter.filter(doc, parse, null, null, null);

		assertNull(doc);  // skip because URL contains wiki

		// second document
		
		NutchDocument doc2 = new NutchDocument();
		doc2.add("type", "text/xml");
		doc2.add("id", "https://foobar.org/baz.xml");
		
		Parse parse2 = mock(Parse.class);
		Metadata metadata2 = new Metadata();
		ParseData parseData2 = new ParseData();
		parseData2.setParseMeta(metadata2);

		// Mock parser response
		when(parse2.getData()).thenReturn(parseData2);
		doc2 = filter.filter(doc2, parse2, null, null, null);

		assertNotNull(doc2);  // skip because URL contains wiki

	}

}

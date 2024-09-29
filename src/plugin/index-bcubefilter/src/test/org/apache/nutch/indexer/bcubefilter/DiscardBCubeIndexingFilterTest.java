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

import java.util.ArrayList;
import java.util.Arrays;

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
		conf.set("indexingfilter.bcube.forbidden.url.patterns", "wiki allDatasets scripts error\\.xml$ \\.rss$ \\.rdf$");
		DiscardBCubeIndexingFilter filter = new DiscardBCubeIndexingFilter();
		filter.setConf(conf);

		// setup a series of test cases with expected result assuming above filtering patterns and MIME types
		ArrayList<Object[]> theCases = new ArrayList<Object[]>(
					   Arrays.asList(
					      // keyword exclusion tests for "wiki", "scripts", "allDatasets
					      new Object[]{"https://en.wikipedia.org/wiki/Apache_Nutch", "text/html", true}, // "wiki" appears
					      new Object[]{"https://en.wikipedia.org/wiki/Apache_Nutch", "text/xml", true},  
					      new Object[]{"https://example.gov/baz.xml", "text/xml", false},  // no forbidden patterns appear in URL
					      new Object[]{"https://example.edu/transcript/janebishop/home", "text/xml", false}, // "script" not "scripts"!
					      new Object[]{"https://example.edu/transcripts/janebishop/home", "text/xml", true}, // yes this has "scripts"
					      new Object[]{"https://example.gov/erdap/allDatasets.html/","text/html", true},   // "allDatasets" exclude

					      // suffix exclusion tests: error.xml, .rss
					      new Object[]{"https://example.gov/server/error.xml", "text/xml", true},  // error.xml is a suffix
					      new Object[]{"https://example.gov/server/errorsxml", "text/xml", false},  // not a suffix "missing '.'"!
					      new Object[]{"https://example.gov/error.xml_extra", "text/xml", false}, // don't exclude (not suffix!)
					      new Object[]{"https://example.gov/data.rdf", "text/xml", true},   // .rdf exclude
					      new Object[]{"https://example.gov/data.rss", "text/xml", true},   // .rss exclude
					      new Object[]{"https://example.gov/datarss", "text/xml", false},   // don't exclude! (missing period)
					      new Object[]{"https://example.gov/test_rss_feed", "text/xml", false},   // don't exclude! (missing period)
					      new Object[]{"https://example.gov/data.rss/baz", "text/xml", false})   // don't exclude! (not suffix!)
								  );
		// iterate through test cases
		for (Object[] aCase : theCases) {
		    String url = (String) aCase[0];
		    String mime = (String) aCase[1];
		    Boolean skipIndexing = (Boolean) aCase[2];

		    NutchDocument doc = new NutchDocument();
		    doc.add("id", url);
		    doc.add("type", mime);
		
		    Parse parse = mock(Parse.class);
		    Metadata metadata = new Metadata();
		    ParseData parseData = new ParseData();
		    parseData.setParseMeta(metadata);

		    // Mock parser response
		    when(parse.getData()).thenReturn(parseData);
		    doc = filter.filter(doc, parse, null, null, null);

		    if (skipIndexing) {
			assertNull(doc);  // if indexing skipping expected, then doc will be null
		    } else {
			assertNotNull(doc); // if indexing skipping NOT expected, then doc will be non-null
		    }
		}
	}
    
}

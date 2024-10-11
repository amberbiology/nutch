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
		conf.set("indexingfilter.bcube.allowed.html.regex", "<pre[^>]*>\\n?((Attributes|Dataset) \\{.*\\}.*\\n?)</pre>");
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

		// second simulated <pre> section
		NutchDocument doc2 = new NutchDocument();
		
		doc2.add("type", "text/html");
		doc2.add("id", "https://notrealurl.example.net/baz");
		
		Parse parse2 = mock(Parse.class);
		Metadata metadata2 = new Metadata();

		String content2 = String.join("\n",
					     "<!DOCTYPE html>",
					     "<html> <pre>",
					     "Dataset {",
					     "    Grid {",
 					     "     ARRAY:",
 					     "        Float64 longitude[yc = 1302][xc = 1069];",
 					     "     MAPS:",
 					     "        Float64 yc[yc = 1302];",
 					     "        Float64 xc[xc = 1069];",
 					     "    } longitude;",
 					     "    Grid {",
 					     "     ARRAY:",
 					     "        Float64 latitude[yc = 1302][xc = 1069];",
 					     "     MAPS:",
 					     "        Float64 yc[yc = 1302];",
 					     "        Float64 xc[xc = 1069];",
 					     "    } latitude;",
 					     "    Float64 projection;",
 					     "    Grid {",
 					     "     ARRAY:",
 					     "        Float64 ice_concentration[time = 1][yc = 1302][xc = 1069];",
 					     "     MAPS:",
 					     "        Float64 time[time = 1];",
 					     "        Float64 yc[yc = 1302];",
 					     "        Float64 xc[xc = 1069];",
 					     "    } ice_concentration;",
 					     "    Float64 time[time = 1];",
 					     "    Float64 xc[xc = 1069];",
 					     "    Float64 yc[yc = 1302];",
 					     "} arcticdata/met/seaiceforecast/forecast/topaz_barents_merged_iceconc_fc-96.nc;",
 					     "</pre>",
					     "</html>");					     

		metadata2.add("raw_content", content2);
		ParseData parseData2 = new ParseData();
		parseData2.setParseMeta(metadata2);

		// Mock parser response
		when(parse2.getData()).thenReturn(parseData2);

		doc2 = filter.filter(doc2, parse2, null, null, null);

		assertNotNull(doc2);

		assertTrue(doc2.getFieldNames().contains("type"));
		NutchField contentTypeField2 = doc2.getField("type");
		
		String typeValue2 = contentTypeField2.getValues().get(0).toString();
		assertTrue(typeValue2.equals("text/html"));
 		
	}

    	@Test
	public void DiscardBCubeIndexingFilter_skip_indexing_url_filters() throws Exception {
		Configuration conf = NutchConfiguration.create();
		conf.setBoolean("moreIndexingFilter.indexMimeTypeParts", true);
		conf.set("indexingfilter.bcube.allowed.mimetypes", "text/html text/xml application/xml");		
		conf.set("indexingfilter.bcube.forbidden.url.patterns", "/errdap/wms/ /oembed/[0-9\\.]+/embed /ows\\?service=WCS /ows\\?service=wfs allDatasets scripts tutorial userguide workshop announcement sitemap wiki error\\.xml$ \\.rss$ \\.rdf$");
		DiscardBCubeIndexingFilter filter = new DiscardBCubeIndexingFilter();
		filter.setConf(conf);

		// setup a series of test cases with expected result assuming above filtering patterns and MIME types
		ArrayList<Object[]> theCases = new ArrayList<Object[]>(
					   Arrays.asList(
					      // keyword exclusion tests for "wiki", "scripts", "allDatasets"
					      new Object[]{"https://en.wikipedia.org/wiki/Apache_Nutch", "text/html", true}, // "wiki" appears
					      new Object[]{"https://en.wikipedia.org/wiki/Apache_Nutch", "text/xml", true},  
					      new Object[]{"https://example.gov/baz.xml", "text/xml", false},  // no forbidden patterns appear in URL
					      new Object[]{"https://example.edu/transcript/janebishop/home", "text/xml", false}, // "script" not "scripts"!
					      new Object[]{"https://example.edu/transcripts/janebishop/home", "text/xml", true}, // yes this has "scripts"
					      new Object[]{"https://example.gov/errdap/allDatasets.html/","text/html", true},   // "allDatasets" exclude
					      new Object[]{"https://example.gov/errdap/alldatasets.html/","text/html", true},   // "allDatasets" exclude, lowercase!
					      new Object[]{"https://example.gov/errdap/wms/stuff/","text/xml", true},   // "/errdap/wms/" exclude

					      new Object[]{"https://example.gov/wp-json/oembed/1.0/embed?url=https%3A%2F%2Fexample.gov%2F&format=xml", "application/xml", true},
					      new Object[]{"https://example.gov/wp-json/oembed/FOO/embed?url=https%3A%2F%2Fexample.gov%2F&format=xml", "application/xml", false},

					      new Object[]{"https://example.edu/foo/ows?service=wfs", "text/xml", true},
					      new Object[]{"https://example.edu/foo/OWS?service=Wfs", "text/xml", true},
					      new Object[]{"https://example.edu/foo/OWS_service=Wfs", "text/xml", false}, // doesn't NOT contain the query
					      
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

/*
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
package org.apache.nutch.urlfilter.regex;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.lang.invoke.MethodHandles;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.urlfilter.api.RegexRule;
import org.apache.nutch.urlfilter.api.RegexURLFilterBase;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filters URLs based on a file of regular expressions using the
 * {@link java.util.regex Java Regex implementation}.
 */
public class RegexURLFilter extends RegexURLFilterBase {

  public static final String URLFILTER_REGEX_FILE = "urlfilter.regex.file";
  public static final String URLFILTER_REGEX_RULES = "urlfilter.regex.rules";

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());

  public RegexURLFilter() {
    super();
  }

  public RegexURLFilter(String filename) throws IOException,
      PatternSyntaxException {
    super(filename);
  }

  RegexURLFilter(Reader reader) throws IOException, IllegalArgumentException {
    super(reader);
  }

  /**
   * Rules specified as a config property will override rules specified as a
   * config file.
   */
  @Override
  protected Reader getRulesReader(Configuration conf) throws IOException {
    String stringRules = conf.get(URLFILTER_REGEX_RULES);
    if (stringRules != null) {
      LOG.info("Reading urlfilter-regex string rules from property: {}",
          URLFILTER_REGEX_RULES);
      return new StringReader(stringRules);
    }
    String fileRules = conf.get(URLFILTER_REGEX_FILE);
    LOG.info("Reading urlfilter-regex rules file: {}", fileRules);
    return conf.getConfResourceAsReader(fileRules);
  }

  // Inherited Javadoc
  @Override
  protected RegexRule createRule(boolean sign, String regex) {
    return new Rule(sign, regex);
  }
  
  @Override
  protected RegexRule createRule(boolean sign, String regex, String hostOrDomain) {
    return new Rule(sign, regex, hostOrDomain);
  }
  
  

  public static void main(String args[]) throws IOException {
    RegexURLFilter filter = new RegexURLFilter();
    filter.setConf(NutchConfiguration.create());
    main(filter, args);
  }

  private class Rule extends RegexRule {

    private Pattern pattern;

    Rule(boolean sign, String regex) {
      this(sign, regex, null);
    }
    
    Rule(boolean sign, String regex, String hostOrDomain) {
      super(sign, regex, hostOrDomain);
      pattern = Pattern.compile(regex);
    }

    @Override
    protected boolean match(String url) {
      return pattern.matcher(url).find();
    }
  }

}

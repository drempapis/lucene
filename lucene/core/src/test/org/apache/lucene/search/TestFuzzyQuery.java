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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.ByteRunnable;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.LevenshteinAutomata;

/** Tests {@link FuzzyQuery}. */
public class TestFuzzyQuery extends LuceneTestCase {

  public void testBasicPrefix() throws Exception {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    addDoc("abc", writer);
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    writer.close();

    FuzzyQuery query = new FuzzyQuery(new Term("field", "abc"), FuzzyQuery.defaultMaxEdits, 1);
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    reader.close();
    directory.close();
  }

  public void testFuzziness() throws Exception {
    Directory directory = newDirectory();
    RandomIndexWriter writer =
        new RandomIndexWriter(
            random(),
            directory,
            newIndexWriterConfig(new MockAnalyzer(random()))
                .setMergePolicy(newMergePolicy(random(), false)));
    addDoc("aaaaa", writer);
    addDoc("aaaab", writer);
    addDoc("aaabb", writer);
    addDoc("aabbb", writer);
    addDoc("abbbb", writer);
    addDoc("bbbbb", writer);
    addDoc("ddddd", writer);

    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    writer.close();

    FuzzyQuery query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMaxEdits, 0);
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);

    // same with prefix
    query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMaxEdits, 1);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMaxEdits, 2);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMaxEdits, 3);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMaxEdits, 4);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(2, hits.length);
    query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMaxEdits, 5);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMaxEdits, 6);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);

    // test scoring
    query = new FuzzyQuery(new Term("field", "bbbbb"), FuzzyQuery.defaultMaxEdits, 0);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("3 documents should match", 3, hits.length);
    List<String> order = Arrays.asList("bbbbb", "abbbb", "aabbb");
    StoredFields storedFields = searcher.storedFields();
    for (int i = 0; i < hits.length; i++) {
      final String term = storedFields.document(hits[i].doc).get("field");
      // System.out.println(hits[i].score);
      assertEquals(order.get(i), term);
    }

    // test pq size by supplying maxExpansions=2
    // This query would normally return 3 documents, because 3 terms match (see above):
    query = new FuzzyQuery(new Term("field", "bbbbb"), FuzzyQuery.defaultMaxEdits, 0, 2, false);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals("only 2 documents should match", 2, hits.length);
    order = Arrays.asList("bbbbb", "abbbb");
    for (int i = 0; i < hits.length; i++) {
      final String term = storedFields.document(hits[i].doc).get("field");
      // System.out.println(hits[i].score);
      assertEquals(order.get(i), term);
    }

    // not similar enough:
    query = new FuzzyQuery(new Term("field", "xxxxx"), FuzzyQuery.defaultMaxEdits, 0);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);
    query =
        new FuzzyQuery(
            new Term("field", "aaccc"),
            FuzzyQuery.defaultMaxEdits,
            0); // edit distance to "aaaaa" = 3
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // query identical to a word in the index:
    query = new FuzzyQuery(new Term("field", "aaaaa"), FuzzyQuery.defaultMaxEdits, 0);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(storedFields.document(hits[0].doc).get("field"), ("aaaaa"));
    // default allows for up to two edits:
    assertEquals(storedFields.document(hits[1].doc).get("field"), ("aaaab"));
    assertEquals(storedFields.document(hits[2].doc).get("field"), ("aaabb"));

    // query similar to a word in the index:
    query = new FuzzyQuery(new Term("field", "aaaac"), FuzzyQuery.defaultMaxEdits, 0);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(storedFields.document(hits[0].doc).get("field"), ("aaaaa"));
    assertEquals(storedFields.document(hits[1].doc).get("field"), ("aaaab"));
    assertEquals(storedFields.document(hits[2].doc).get("field"), ("aaabb"));

    // now with prefix
    query = new FuzzyQuery(new Term("field", "aaaac"), FuzzyQuery.defaultMaxEdits, 1);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(storedFields.document(hits[0].doc).get("field"), ("aaaaa"));
    assertEquals(storedFields.document(hits[1].doc).get("field"), ("aaaab"));
    assertEquals(storedFields.document(hits[2].doc).get("field"), ("aaabb"));
    query = new FuzzyQuery(new Term("field", "aaaac"), FuzzyQuery.defaultMaxEdits, 2);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(storedFields.document(hits[0].doc).get("field"), ("aaaaa"));
    assertEquals(storedFields.document(hits[1].doc).get("field"), ("aaaab"));
    assertEquals(storedFields.document(hits[2].doc).get("field"), ("aaabb"));
    query = new FuzzyQuery(new Term("field", "aaaac"), FuzzyQuery.defaultMaxEdits, 3);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    assertEquals(storedFields.document(hits[0].doc).get("field"), ("aaaaa"));
    assertEquals(storedFields.document(hits[1].doc).get("field"), ("aaaab"));
    assertEquals(storedFields.document(hits[2].doc).get("field"), ("aaabb"));
    query = new FuzzyQuery(new Term("field", "aaaac"), FuzzyQuery.defaultMaxEdits, 4);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(2, hits.length);
    assertEquals(storedFields.document(hits[0].doc).get("field"), ("aaaaa"));
    assertEquals(storedFields.document(hits[1].doc).get("field"), ("aaaab"));
    query = new FuzzyQuery(new Term("field", "aaaac"), FuzzyQuery.defaultMaxEdits, 5);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);

    query = new FuzzyQuery(new Term("field", "ddddX"), FuzzyQuery.defaultMaxEdits, 0);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(storedFields.document(hits[0].doc).get("field"), ("ddddd"));

    // now with prefix
    query = new FuzzyQuery(new Term("field", "ddddX"), FuzzyQuery.defaultMaxEdits, 1);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(storedFields.document(hits[0].doc).get("field"), ("ddddd"));
    query = new FuzzyQuery(new Term("field", "ddddX"), FuzzyQuery.defaultMaxEdits, 2);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(storedFields.document(hits[0].doc).get("field"), ("ddddd"));
    query = new FuzzyQuery(new Term("field", "ddddX"), FuzzyQuery.defaultMaxEdits, 3);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(storedFields.document(hits[0].doc).get("field"), ("ddddd"));
    query = new FuzzyQuery(new Term("field", "ddddX"), FuzzyQuery.defaultMaxEdits, 4);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals(storedFields.document(hits[0].doc).get("field"), ("ddddd"));
    query = new FuzzyQuery(new Term("field", "ddddX"), FuzzyQuery.defaultMaxEdits, 5);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);

    // different field = no match:
    query = new FuzzyQuery(new Term("anotherfield", "ddddX"), FuzzyQuery.defaultMaxEdits, 0);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(0, hits.length);

    reader.close();
    directory.close();
  }

  public void testPrefixLengthEqualStringLength() throws Exception {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    addDoc("b*a", writer);
    addDoc("b*ab", writer);
    addDoc("b*abc", writer);
    addDoc("b*abcd", writer);
    String multibyte = "아프리카코끼리속";
    addDoc(multibyte, writer);
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    writer.close();

    int maxEdits = 0;
    int prefixLength = 3;
    FuzzyQuery query = new FuzzyQuery(new Term("field", "b*a"), maxEdits, prefixLength);
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);

    maxEdits = 1;
    query = new FuzzyQuery(new Term("field", "b*a"), maxEdits, prefixLength);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(2, hits.length);

    maxEdits = 2;
    query = new FuzzyQuery(new Term("field", "b*a"), maxEdits, prefixLength);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);

    maxEdits = 1;
    prefixLength = multibyte.length() - 1;
    query =
        new FuzzyQuery(
            new Term("field", multibyte.substring(0, prefixLength)), maxEdits, prefixLength);
    hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(1, hits.length);

    reader.close();
    directory.close();
  }

  public void test2() throws Exception {
    Directory directory = newDirectory();
    RandomIndexWriter writer =
        new RandomIndexWriter(
            random(), directory, new MockAnalyzer(random(), MockTokenizer.KEYWORD, false));
    addDoc("LANGE", writer);
    addDoc("LUETH", writer);
    addDoc("PIRSING", writer);
    addDoc("RIEGEL", writer);
    addDoc("TRZECZIAK", writer);
    addDoc("WALKER", writer);
    addDoc("WBR", writer);
    addDoc("WE", writer);
    addDoc("WEB", writer);
    addDoc("WEBE", writer);
    addDoc("WEBER", writer);
    addDoc("WEBERE", writer);
    addDoc("WEBREE", writer);
    addDoc("WEBEREI", writer);
    addDoc("WBRE", writer);
    addDoc("WITTKOPF", writer);
    addDoc("WOJNAROWSKI", writer);
    addDoc("WRICKE", writer);

    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    writer.close();

    FuzzyQuery query = new FuzzyQuery(new Term("field", "WEBER"), 2, 1);
    // query.setRewriteMethod(FuzzyQuery.SCORING_BOOLEAN_QUERY_REWRITE);
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(8, hits.length);

    reader.close();
    directory.close();
  }

  public void testSingleQueryExactMatchScoresHighest() throws Exception {
    // See issue LUCENE-329 - IDF shouldn't wreck similarity ranking
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    addDoc("smith", writer);
    addDoc("smith", writer);
    addDoc("smith", writer);
    addDoc("smith", writer);
    addDoc("smith", writer);
    addDoc("smith", writer);
    addDoc("smythe", writer);
    addDoc("smdssasd", writer);

    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    searcher.setSimilarity(
        new ClassicSimilarity()); // avoid randomisation of similarity algo by test framework
    writer.close();
    String[] searchTerms = {"smith", "smythe", "smdssasd"};
    StoredFields storedFields = reader.storedFields();
    for (String searchTerm : searchTerms) {
      FuzzyQuery query = new FuzzyQuery(new Term("field", searchTerm), 2, 1);
      ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
      Document bestDoc = storedFields.document(hits[0].doc);
      assertTrue(hits.length > 0);
      String topMatch = bestDoc.get("field");
      assertEquals(searchTerm, topMatch);
      if (hits.length > 1) {
        Document worstDoc = storedFields.document(hits[hits.length - 1].doc);
        String worstMatch = worstDoc.get("field");
        assertNotSame(searchTerm, worstMatch);
      }
    }
    reader.close();
    directory.close();
  }

  public void testMultipleQueriesIdfWorks() throws Exception {
    // With issue LUCENE-329 - it could be argued a
    // MultiTermQuery.TopTermsBoostOnlyBooleanQueryRewrite
    // is the solution as it disables IDF.
    // However - IDF is still useful as in this case where there are multiple FuzzyQueries.
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);

    addDoc("michael smith", writer);
    addDoc("michael lucero", writer);
    addDoc("doug cutting", writer);
    addDoc("doug cuttin", writer);
    addDoc("michael wardle", writer);
    addDoc("micheal vegas", writer);
    addDoc("michael lydon", writer);

    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    searcher.setSimilarity(
        new ClassicSimilarity()); // avoid randomisation of similarity algo by test framework

    writer.close();

    BooleanQuery.Builder query = new BooleanQuery.Builder();
    String commonSearchTerm = "michael";
    FuzzyQuery commonQuery = new FuzzyQuery(new Term("field", commonSearchTerm), 2, 1);
    query.add(commonQuery, Occur.SHOULD);

    String rareSearchTerm = "cutting";
    FuzzyQuery rareQuery = new FuzzyQuery(new Term("field", rareSearchTerm), 2, 1);
    query.add(rareQuery, Occur.SHOULD);
    ScoreDoc[] hits = searcher.search(query.build(), 1000).scoreDocs;

    // Matches on the rare surname should be worth more than matches on the common forename
    assertEquals(7, hits.length);
    Document bestDoc = searcher.storedFields().document(hits[0].doc);
    String topMatch = bestDoc.get("field");
    assertTrue(topMatch.contains(rareSearchTerm));

    Document runnerUpDoc = searcher.storedFields().document(hits[1].doc);
    String runnerUpMatch = runnerUpDoc.get("field");
    assertTrue(runnerUpMatch.contains("cuttin"));

    Document worstDoc = searcher.storedFields().document(hits[hits.length - 1].doc);
    String worstMatch = worstDoc.get("field");
    assertTrue(worstMatch.contains("micheal")); // misspelling of common name

    reader.close();
    directory.close();
  }

  /**
   * MultiTermQuery provides (via attribute) information about which values must be competitive to
   * enter the priority queue.
   *
   * <p>FuzzyQuery optimizes itself around this information, if the attribute is not implemented
   * correctly, there will be problems!
   */
  public void testTieBreaker() throws Exception {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    addDoc("a123456", writer);
    addDoc("c123456", writer);
    addDoc("d123456", writer);
    addDoc("e123456", writer);

    Directory directory2 = newDirectory();
    RandomIndexWriter writer2 = new RandomIndexWriter(random(), directory2);
    addDoc("a123456", writer2);
    addDoc("b123456", writer2);
    addDoc("b123456", writer2);
    addDoc("b123456", writer2);
    addDoc("c123456", writer2);
    addDoc("f123456", writer2);

    IndexReader ir1 = writer.getReader();
    IndexReader ir2 = writer2.getReader();

    MultiReader mr = new MultiReader(ir1, ir2);
    IndexSearcher searcher = newSearcher(mr);
    FuzzyQuery fq = new FuzzyQuery(new Term("field", "z123456"), 1, 0, 2, false);
    TopDocs docs = searcher.search(fq, 2);
    assertEquals(5, docs.totalHits.value()); // 5 docs, from the a and b's
    mr.close();
    ir1.close();
    ir2.close();
    writer.close();
    writer2.close();
    directory.close();
    directory2.close();
  }

  /** Test the TopTermsBoostOnlyBooleanQueryRewrite rewrite method. */
  public void testBoostOnlyRewrite() throws Exception {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    addDoc("Lucene", writer);
    addDoc("Lucene", writer);
    addDoc("Lucenne", writer);

    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    writer.close();

    FuzzyQuery query =
        new FuzzyQuery(
            new Term("field", "lucene"),
            FuzzyQuery.defaultMaxEdits,
            FuzzyQuery.defaultPrefixLength,
            FuzzyQuery.defaultMaxExpansions,
            FuzzyQuery.defaultTranspositions,
            new MultiTermQuery.TopTermsBoostOnlyBooleanQueryRewrite(50));
    ScoreDoc[] hits = searcher.search(query, 1000).scoreDocs;
    assertEquals(3, hits.length);
    // normally, 'Lucenne' would be the first result as IDF will skew the score.
    assertEquals("Lucene", reader.storedFields().document(hits[0].doc).get("field"));
    assertEquals("Lucene", reader.storedFields().document(hits[1].doc).get("field"));
    assertEquals("Lucenne", reader.storedFields().document(hits[2].doc).get("field"));
    reader.close();
    directory.close();
  }

  public void testGiga() throws Exception {

    Directory index = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), index);

    addDoc("Lucene in Action", w);
    addDoc("Lucene for Dummies", w);

    // addDoc("Giga", w);
    addDoc("Giga byte", w);

    addDoc("ManagingGigabytesManagingGigabyte", w);
    addDoc("ManagingGigabytesManagingGigabytes", w);

    addDoc("The Art of Computer Science", w);
    addDoc("J. K. Rowling", w);
    addDoc("JK Rowling", w);
    addDoc("Joanne K Roling", w);
    addDoc("Bruce Willis", w);
    addDoc("Willis bruce", w);
    addDoc("Brute willis", w);
    addDoc("B. willis", w);
    IndexReader r = w.getReader();
    w.close();

    Query q = new FuzzyQuery(new Term("field", "giga"), 0);

    // 3. search
    IndexSearcher searcher = newSearcher(r);
    ScoreDoc[] hits = searcher.search(q, 10).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals("Giga byte", searcher.storedFields().document(hits[0].doc).get("field"));
    r.close();
    w.close();
    index.close();
  }

  public void testDistanceAsEditsSearching() throws Exception {
    Directory index = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), index);
    addDoc("foobar", w);
    addDoc("test", w);
    addDoc("working", w);
    IndexReader reader = w.getReader();
    IndexSearcher searcher = newSearcher(reader);
    w.close();

    FuzzyQuery q = new FuzzyQuery(new Term("field", "fouba"), 2);
    ScoreDoc[] hits = searcher.search(q, 10).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals("foobar", searcher.storedFields().document(hits[0].doc).get("field"));

    q = new FuzzyQuery(new Term("field", "foubara"), 2);
    hits = searcher.search(q, 10).scoreDocs;
    assertEquals(1, hits.length);
    assertEquals("foobar", searcher.storedFields().document(hits[0].doc).get("field"));

    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new FuzzyQuery(new Term("field", "t"), 3);
        });

    reader.close();
    index.close();
  }

  public void testValidation() {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new FuzzyQuery(new Term("field", "foo"), -1, 0, 1, false);
            });
    assertTrue(expected.getMessage().contains("maxEdits"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new FuzzyQuery(
                  new Term("field", "foo"),
                  LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE + 1,
                  0,
                  1,
                  false);
            });
    assertTrue(expected.getMessage().contains("maxEdits must be between"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new FuzzyQuery(new Term("field", "foo"), 1, -1, 1, false);
            });
    assertTrue(expected.getMessage().contains("prefixLength cannot be negative"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new FuzzyQuery(new Term("field", "foo"), 1, 0, -1, false);
            });
    assertTrue(expected.getMessage().contains("maxExpansions must be positive"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              new FuzzyQuery(new Term("field", "foo"), 1, 0, -1, false);
            });
    assertTrue(expected.getMessage().contains("maxExpansions must be positive"));
  }

  public void testRamBytesUsedIsStableAndBoundedToRetainedState() throws Exception {
    FuzzyQuery q = new FuzzyQuery(new Term("field", "hello"), 2);
    long expected =
        RamUsageEstimator.shallowSizeOfInstance(FuzzyQuery.class) + q.getTerm().ramBytesUsed();
    assertEquals(
        "ramBytesUsed must equal shallow size of FuzzyQuery + term.ramBytesUsed()",
        expected,
        q.ramBytesUsed());
    assertEquals("ramBytesUsed must be stable across reads", expected, q.ramBytesUsed());

    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    addDoc("hello", writer);
    addDoc("world", writer);
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    writer.close();

    searcher.search(q, 10);

    assertEquals("ramBytesUsed must not change after a search", expected, q.ramBytesUsed());
    searcher.search(q, 10);
    assertEquals("ramBytesUsed must not change after a second search", expected, q.ramBytesUsed());

    reader.close();
    directory.close();
  }

  public void testRamBytesUsedNotAffectedByVisitOrSearch() throws Exception {
    FuzzyQuery q = new FuzzyQuery(new Term("field", "hello"), 2);
    long before = q.ramBytesUsed();

    q.visit(QueryVisitor.EMPTY_VISITOR);
    assertEquals("visit() must not affect ramBytesUsed", before, q.ramBytesUsed());

    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    addDoc("hello", writer);
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    writer.close();

    searcher.search(q, 10);
    assertEquals("search() must not affect ramBytesUsed", before, q.ramBytesUsed());

    reader.close();
    directory.close();
  }

  public void testComputeAutomataRamBytesMaxEditsZero() {
    FuzzyQuery q = new FuzzyQuery(new Term("field", "hello"), 0);
    assertEquals(
        "maxEdits=0 has no automata to count",
        0L,
        q.computeAutomataRamBytes(new AttributeSource()));
  }

  public void testComputeAutomataRamBytesPositive() {
    FuzzyQuery q = new FuzzyQuery(new Term("field", "hello"), 2);
    long withAtts = q.computeAutomataRamBytes(new AttributeSource());
    assertTrue("expected a positive automata cost, got " + withAtts, withAtts > 0L);
  }

  public void testComputeAutomataRamBytesGrowsWithMaxEdits() {
    FuzzyQuery q1 = new FuzzyQuery(new Term("field", "hello"), 1);
    FuzzyQuery q2 = new FuzzyQuery(new Term("field", "hello"), 2);
    long r1 = q1.computeAutomataRamBytes(new AttributeSource());
    long r2 = q2.computeAutomataRamBytes(new AttributeSource());
    assertTrue(
        "q2.computeAutomataRamBytes ("
            + r2
            + ") should exceed q1.computeAutomataRamBytes ("
            + r1
            + ")",
        r2 > r1);
  }

  public void testComputeAutomataRamBytesDoesNotMutateRamBytesUsed() {
    FuzzyQuery q = new FuzzyQuery(new Term("field", "hello"), 2);
    long before = q.ramBytesUsed();
    q.computeAutomataRamBytes(new AttributeSource());
    q.computeAutomataRamBytes(new AttributeSource());
    assertEquals("computeAutomataRamBytes must not mutate ramBytesUsed", before, q.ramBytesUsed());
  }

  public void testComputeAutomataRamBytesSharesWithGetTermsEnum() throws Exception {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    addDoc("hello", writer);
    addDoc("help", writer);
    IndexReader reader = writer.getReader();
    writer.close();

    FuzzyQuery q = new FuzzyQuery(new Term("field", "hello"), 2);

    // Measure first, then execute: the AttributeSource carries the pre-built automata to the
    // FuzzyTermsEnum, which should reuse the same array instance (no rebuild).
    AttributeSource atts = new AttributeSource();
    long sum = q.computeAutomataRamBytes(atts);
    assertTrue(sum > 0L);

    FuzzyTermsEnum.AutomatonAttribute aa =
        atts.getAttribute(FuzzyTermsEnum.AutomatonAttribute.class);
    assertNotNull("AutomatonAttribute must be populated after computeAutomataRamBytes", aa);
    CompiledAutomaton[] expected = aa.getAutomata();
    assertNotNull(expected);

    // Drive a FuzzyTermsEnum through the same attribute source; it must reuse the automata.
    LeafReader leaf = reader.leaves().get(0).reader();
    Terms terms = leaf.terms("field");
    assertNotNull(terms);
    new FuzzyTermsEnum(terms, atts, q.getTerm(), 2, 0, true);

    assertSame(
        "FuzzyTermsEnum must reuse the automata stored by computeAutomataRamBytes",
        expected,
        aa.getAutomata());
    assertEquals(
        "Calling computeAutomataRamBytes again on the same atts must not rebuild",
        sum,
        q.computeAutomataRamBytes(atts));
    assertSame(
        "Automata array identity must be preserved across repeat calls",
        expected,
        aa.getAutomata());

    reader.close();
    directory.close();
  }

  public void testComputeAutomataRamBytesDoesNotShareAcrossDifferentAttributeSources() {
    FuzzyQuery q = new FuzzyQuery(new Term("field", "hello"), 2);

    AttributeSource a = new AttributeSource();
    AttributeSource b = new AttributeSource();

    long sumA = q.computeAutomataRamBytes(a);
    long sumB = q.computeAutomataRamBytes(b);
    assertEquals(
        "same query must report the same total across independent AttributeSources", sumA, sumB);

    FuzzyTermsEnum.AutomatonAttribute aa = a.getAttribute(FuzzyTermsEnum.AutomatonAttribute.class);
    FuzzyTermsEnum.AutomatonAttribute bb = b.getAttribute(FuzzyTermsEnum.AutomatonAttribute.class);
    assertNotNull(aa);
    assertNotNull(bb);
    assertNotSame(
        "Independent AttributeSources must hold independent automata arrays",
        aa.getAutomata(),
        bb.getAutomata());
  }

  public void testPreflightThenReuseAcrossSegments() throws Exception {
    Directory directory = newDirectory();
    IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory, iwc);
    addDoc("hello", writer);
    writer.commit();
    addDoc("help", writer);
    writer.commit();
    addDoc("world", writer);
    IndexReader reader = writer.getReader();
    writer.close();
    assumeTrue("need >= 2 segments for this test", reader.leaves().size() >= 2);

    FuzzyQuery q = new FuzzyQuery(new Term("field", "hello"), 2);

    AttributeSource atts = new AttributeSource();
    long preflightBytes = q.computeAutomataRamBytes(atts);
    assertTrue(preflightBytes > 0L);

    CompiledAutomaton[] expected =
        atts.getAttribute(FuzzyTermsEnum.AutomatonAttribute.class).getAutomata();
    assertNotNull(expected);

    for (LeafReaderContext ctx : reader.leaves()) {
      Terms terms = ctx.reader().terms("field");
      assertNotNull(terms);
      TermsEnum te = q.getTermsEnum(terms, atts);
      assertNotNull(te);
      assertSame(
          "automata array must be shared across segments",
          expected,
          atts.getAttribute(FuzzyTermsEnum.AutomatonAttribute.class).getAutomata());
    }

    assertEquals(
        "second computeAutomataRamBytes on primed atts must not rebuild",
        preflightBytes,
        q.computeAutomataRamBytes(atts));
    assertSame(
        "automata array identity must be preserved after subsequent computeAutomataRamBytes",
        expected,
        atts.getAttribute(FuzzyTermsEnum.AutomatonAttribute.class).getAutomata());

    reader.close();
    directory.close();
  }

  public void testInFlightObservationViaSubclass() throws Exception {
    Directory directory = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), directory);
    addDoc("hello", writer);
    addDoc("help", writer);
    addDoc("world", writer);
    IndexReader reader = writer.getReader();
    IndexSearcher searcher = newSearcher(reader);
    writer.close();

    AtomicLong observedBytes = new AtomicLong(-1L);
    AtomicReference<CompiledAutomaton[]> observedArray = new AtomicReference<>();

    class ObservingFuzzyQuery extends FuzzyQuery {
      ObservingFuzzyQuery(Term t) {
        super(t, 2);
      }

      @Override
      protected TermsEnum getTermsEnum(Terms terms, AttributeSource atts) throws IOException {
        TermsEnum te = super.getTermsEnum(terms, atts);
        CompiledAutomaton[] before =
            atts.getAttribute(FuzzyTermsEnum.AutomatonAttribute.class).getAutomata();
        long bytes = computeAutomataRamBytes(atts);
        CompiledAutomaton[] after =
            atts.getAttribute(FuzzyTermsEnum.AutomatonAttribute.class).getAutomata();
        assertSame("observation must not rebuild automata", before, after);
        observedBytes.compareAndSet(-1L, bytes);
        observedArray.compareAndSet(null, after);
        return te;
      }
    }

    Term term = new Term("field", "hello");
    TopDocs observedHits = searcher.search(new ObservingFuzzyQuery(term), 10);

    long expected = new FuzzyQuery(term, 2).computeAutomataRamBytes(new AttributeSource());

    assertEquals("subclass observation matches a fresh pre-flight", expected, observedBytes.get());
    assertNotNull(observedArray.get());

    TopDocs vanillaHits = searcher.search(new FuzzyQuery(term, 2), 10);
    assertEquals(vanillaHits.totalHits.value(), observedHits.totalHits.value());

    reader.close();
    directory.close();
  }

  private void addDoc(String text, RandomIndexWriter writer) throws IOException {
    Document doc = new Document();
    doc.add(newTextField("field", text, Field.Store.YES));
    writer.addDocument(doc);
  }

  private String randomSimpleString(int digits) {
    int termLength = TestUtil.nextInt(random(), 1, 8);
    char[] chars = new char[termLength];
    for (int i = 0; i < termLength; i++) {
      chars[i] = (char) ('a' + random().nextInt(digits));
    }
    return new String(chars);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testRandom() throws Exception {
    int digits = TestUtil.nextInt(random(), 2, 3);
    // underestimated total number of unique terms that randomSimpleString
    // maybe generate, it assumes all terms have a length of 7
    int vocabularySize = digits << 7;
    int numTerms = Math.min(atLeast(100), vocabularySize);
    Set<String> terms = new HashSet<>();
    while (terms.size() < numTerms) {
      terms.add(randomSimpleString(digits));
    }

    Directory dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    for (String term : terms) {
      Document doc = new Document();
      doc.add(new StringField("field", term, Field.Store.YES));
      w.addDocument(doc);
    }
    DirectoryReader r = w.getReader();
    w.close();
    // System.out.println("TEST: reader=" + r);
    IndexSearcher s = newSearcher(r);
    int iters = atLeast(200);
    for (int iter = 0; iter < iters; iter++) {
      String queryTerm = randomSimpleString(digits);
      int prefixLength = random().nextInt(queryTerm.length());
      String queryPrefix = queryTerm.substring(0, prefixLength);

      // we don't look at scores here:
      List<TermAndScore>[] expected = new List[3];
      for (int ed = 0; ed < 3; ed++) {
        expected[ed] = new ArrayList<>();
      }
      for (String term : terms) {
        if (term.startsWith(queryPrefix) == false) {
          continue;
        }
        int ed = getDistance(term, queryTerm);
        float score = 1f - (float) ed / (float) Math.min(queryTerm.length(), term.length());
        while (ed < 3) {
          expected[ed].add(new TermAndScore(term, score));
          ed++;
        }
      }

      for (int ed = 0; ed < 3; ed++) {
        Collections.sort(expected[ed]);
        int queueSize = TestUtil.nextInt(random(), 1, terms.size());
        /*
        System.out.println("\nTEST: query=" + queryTerm + " ed=" + ed + " queueSize=" + queueSize + " vs expected match size=" + expected[ed].size() + " prefixLength=" + prefixLength);
        for(TermAndScore ent : expected[ed]) {
          System.out.println("  " + ent);
        }
        */
        FuzzyQuery query =
            new FuzzyQuery(new Term("field", queryTerm), ed, prefixLength, queueSize, true);
        TopDocs hits = s.search(query, terms.size());
        Set<String> actual = new HashSet<>();
        StoredFields storedFields = s.storedFields();
        for (ScoreDoc hit : hits.scoreDocs) {
          Document doc = storedFields.document(hit.doc);
          actual.add(doc.get("field"));
          // System.out.println("   actual: " + doc.get("field") + " score=" + hit.score);
        }
        Set<String> expectedTop = new HashSet<>();
        int limit = Math.min(queueSize, expected[ed].size());
        for (int i = 0; i < limit; i++) {
          expectedTop.add(expected[ed].get(i).term);
        }

        if (actual.equals(expectedTop) == false) {
          StringBuilder sb = new StringBuilder();
          sb.append(
              "FAILED: query="
                  + queryTerm
                  + " ed="
                  + ed
                  + " queueSize="
                  + queueSize
                  + " vs expected match size="
                  + expected[ed].size()
                  + " prefixLength="
                  + prefixLength
                  + "\n");

          boolean first = true;
          for (String term : actual) {
            if (expectedTop.contains(term) == false) {
              if (first) {
                sb.append("  these matched but shouldn't:\n");
                first = false;
              }
              sb.append("    " + term + "\n");
            }
          }
          first = true;
          for (String term : expectedTop) {
            if (actual.contains(term) == false) {
              if (first) {
                sb.append("  these did not match but should:\n");
                first = false;
              }
              sb.append("    " + term + "\n");
            }
          }
          throw new AssertionError(sb.toString());
        }
      }
    }

    IOUtils.close(r, dir);
  }

  private record TermAndScore(String term, float score) implements Comparable<TermAndScore> {

    @Override
    public int compareTo(TermAndScore other) {
      // higher score sorts first, and if scores are tied, lower term sorts first
      if (score > other.score) {
        return -1;
      } else if (score < other.score) {
        return 1;
      } else {
        return term.compareTo(other.term);
      }
    }
  }

  // Poached from LuceneLevenshteinDistance.java (from suggest module): it supports transpositions
  // (treats them as ed=1, not ed=2)
  private static int getDistance(String target, String other) {
    IntsRef targetPoints;
    IntsRef otherPoints;
    int n;
    int[][] d; // cost array

    // NOTE: if we cared, we could 3*m space instead of m*n space, similar to
    // what LevenshteinDistance does, except cycling thru a ring of three
    // horizontal cost arrays... but this comparator is never actually used by
    // DirectSpellChecker, it's only used for merging results from multiple shards
    // in "distributed spellcheck", and it's inefficient in other ways too...

    // cheaper to do this up front once
    targetPoints = toIntsRef(target);
    otherPoints = toIntsRef(other);
    n = targetPoints.length;
    final int m = otherPoints.length;
    d = new int[n + 1][m + 1];

    if (n == 0 || m == 0) {
      if (n == m) {
        return 0;
      } else {
        return Math.max(n, m);
      }
    }

    // indexes into strings s and t
    int i; // iterates through s
    int j; // iterates through t

    int t_j; // jth character of t

    int cost; // cost

    for (i = 0; i <= n; i++) {
      d[i][0] = i;
    }

    for (j = 0; j <= m; j++) {
      d[0][j] = j;
    }

    for (j = 1; j <= m; j++) {
      t_j = otherPoints.ints[j - 1];

      for (i = 1; i <= n; i++) {
        cost = targetPoints.ints[i - 1] == t_j ? 0 : 1;
        // minimum of cell to the left+1, to the top+1, diagonally left and up +cost
        d[i][j] = Math.min(Math.min(d[i - 1][j] + 1, d[i][j - 1] + 1), d[i - 1][j - 1] + cost);
        // transposition
        if (i > 1
            && j > 1
            && targetPoints.ints[i - 1] == otherPoints.ints[j - 2]
            && targetPoints.ints[i - 2] == otherPoints.ints[j - 1]) {
          d[i][j] = Math.min(d[i][j], d[i - 2][j - 2] + cost);
        }
      }
    }

    return d[n][m];
  }

  private static IntsRef toIntsRef(String s) {
    IntsRef ref = new IntsRef(s.length()); // worst case
    int utf16Len = s.length();
    for (int i = 0, cp = 0; i < utf16Len; i += Character.charCount(cp)) {
      cp = ref.ints[ref.length++] = Character.codePointAt(s, i);
    }
    return ref;
  }

  public void testVisitor() {
    FuzzyQuery q = new FuzzyQuery(new Term("field", "blob"), 2);
    AtomicBoolean visited = new AtomicBoolean(false);
    q.visit(
        new QueryVisitor() {
          @Override
          public void consumeTermsMatching(
              Query query, String field, Supplier<ByteRunnable> automaton) {
            visited.set(true);
            ByteRunnable a = automaton.get();
            assertMatches(a, "blob");
            assertMatches(a, "bolb");
            assertMatches(a, "blobby");
            assertNoMatches(a, "bolbby");
          }
        });
    assertTrue(visited.get());
  }

  private static void assertMatches(ByteRunnable automaton, String text) {
    BytesRef b = newBytesRef(text);
    assertTrue(automaton.run(b.bytes, b.offset, b.length));
  }

  private static void assertNoMatches(ByteRunnable automaton, String text) {
    BytesRef b = newBytesRef(text);
    assertFalse(automaton.run(b.bytes, b.offset, b.length));
  }
}

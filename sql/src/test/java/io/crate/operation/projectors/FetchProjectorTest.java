/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.projectors;

import com.carrotsearch.hppc.*;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.analyze.symbol.FetchReference;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.CollectionBucket;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowN;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.operation.projectors.fetch.FetchOperation;
import io.crate.operation.projectors.fetch.FetchProjector;
import io.crate.operation.projectors.fetch.FetchProjectorContext;
import io.crate.planner.node.fetch.FetchSource;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.RowGenerator;
import io.crate.testing.RowSender;
import io.crate.testing.T3;
import io.crate.testing.TestingHelpers;
import io.crate.types.LongType;
import io.crate.types.StringType;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;

public class FetchProjectorTest extends CrateUnitTest {

    private ExecutorService executorService;
    private DummyFetchOperation fetchOperation;

    @Before
    public void before() throws Exception {
        executorService = Executors.newFixedThreadPool(2);
        // dummy FetchOperation that returns buckets for each reader-id where each row contains a column that is the same as the docId
        fetchOperation = new DummyFetchOperation();
    }

    @After
    public void after() throws Exception {
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.SECONDS);
    }

    @Test
    public void testPauseSupport() throws Exception {
        final CollectingRowReceiver rowReceiver = CollectingRowReceiver.withPauseAfter(2);
        int fetchSize = random().nextInt(20);
        FetchProjector fetchProjector = prepareFetchProjector(fetchSize, rowReceiver, fetchOperation, false);
        final RowSender rowSender = new RowSender(RowGenerator.range(0, 10), fetchProjector, MoreExecutors.directExecutor());
        rowSender.run();

        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertThat(rowReceiver.rows.size(), is(2));
                assertThat(rowReceiver.numPauseProcessed(), is(1));
            }
        });
        assertThat(rowReceiver.getNumFailOrFinishCalls(), is(0));
        rowReceiver.resumeUpstream(false);
        assertThat(TestingHelpers.printedTable(rowReceiver.result()),
            is("0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n"));
    }

    @Test
    public void testMultipleFetchRequests() throws Throwable {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        int fetchSize = 3;
        FetchProjector fetchProjector = prepareFetchProjector(fetchSize, rowReceiver, fetchOperation, false);
        final RowSender rowSender = new RowSender(RowGenerator.range(0, 10), fetchProjector, MoreExecutors.directExecutor());
        rowSender.run();

        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertThat(rowSender.numPauses(), is(3));
            }
        });
        assertThat(fetchOperation.numFetches, Matchers.greaterThan(1));

        Bucket projected = rowReceiver.result();
        assertThat(projected.size(), is(10));

        int iterateLength = Iterables.size(rowReceiver.result());
        assertThat(iterateLength, is(10));
    }

    @Test
    public void testMultipleFetchSources() throws Throwable {
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        int fetchSize = 3;
        FetchProjector fetchProjector = prepareFetchProjector(fetchSize, rowReceiver, fetchOperation, true);
        final RowSender rowSender = new RowSender(rowRangeMultipleColumns(), fetchProjector, MoreExecutors.directExecutor());
        rowSender.run();

        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertThat(rowSender.numPauses(), is(6));
            }
        });
//        assertThat(fetchOperation.numFetches, Matchers.greaterThan(12));

        Bucket projected = rowReceiver.result();
        assertThat(projected.size(), is(20));

        int iterateLength = Iterables.size(rowReceiver.result());
        assertThat(iterateLength, is(20));
    }

    private FetchProjector prepareFetchProjector(int fetchSize,
                                                 CollectingRowReceiver rowReceiver,
                                                 FetchOperation fetchOperation,
                                                 boolean multipleSources) {
        FetchProjector pipe = new FetchProjector(fetchOperation,
                                                 executorService,
                                                 TestingHelpers.getFunctions(),
                                                 multipleSources ? buildOutputSymbolsMultipleSources() :
                                                     buildOutputSymbols(),
                                                 multipleSources ? buildFetchProjectorContextMultipleSources() :
                                                     buildFetchProjectorContext(),
                                                 fetchSize);
        pipe.downstream(rowReceiver);
        pipe.prepare();
        return pipe;
    }

    private FetchProjectorContext buildFetchProjectorContext() {
        Map<String, IntSet> nodeToReaderIds = new HashMap<>(2);
        IntSet nodeReadersNodeOne = new IntHashSet();
        nodeReadersNodeOne.add(0);
        IntSet nodeReadersNodeTwo = new IntHashSet();
        nodeReadersNodeTwo.add(2);
        nodeToReaderIds.put("nodeOne", nodeReadersNodeOne);
        nodeToReaderIds.put("nodeTwo", nodeReadersNodeTwo);

        TreeMap<Integer, String> readerIndices = new TreeMap<>();
        readerIndices.put(0, "t1");

        Map<String, TableIdent> indexToTable = new HashMap<>(1);
        indexToTable.put("t1", T3.T1_INFO.ident());

        ReferenceIdent referenceIdent = new ReferenceIdent(T3.T1_INFO.ident(), "x");
        Reference reference = new Reference(referenceIdent,
            RowGranularity.DOC,
            LongType.INSTANCE,
            ColumnPolicy.STRICT,
            Reference.IndexType.NOT_ANALYZED,
            true);

        Map<TableIdent, FetchSource> tableToFetchSource = new HashMap<>(2);
        FetchSource fetchSource = new FetchSource(Collections.<Reference>emptyList(),
            Collections.singletonList(new InputColumn(0)),
            Collections.singletonList(reference));
        tableToFetchSource.put(T3.T1_INFO.ident(), fetchSource);

        return new FetchProjectorContext(
            tableToFetchSource,
            nodeToReaderIds,
            readerIndices,
            indexToTable
        );
    }

    private FetchProjectorContext buildFetchProjectorContextMultipleSources() {
        Map<String, IntSet> nodeToReaderIds = new HashMap<>(2);
        IntSet nodeReadersNodeOne = new IntHashSet();
        nodeReadersNodeOne.add(0);
        nodeReadersNodeOne.add(1);
        IntSet nodeReadersNodeTwo = new IntHashSet();
        nodeReadersNodeTwo.add(2);
        nodeReadersNodeTwo.add(3);
        nodeToReaderIds.put("nodeOne", nodeReadersNodeOne);
        nodeToReaderIds.put("nodeTwo", nodeReadersNodeTwo);

        TreeMap<Integer, String> readerIndices = new TreeMap<>();
        readerIndices.put(0, "t1");
        readerIndices.put(2, "t1");
        readerIndices.put(1, "t2");
        readerIndices.put(3, "t2");

        Map<String, TableIdent> indexToTable = new HashMap<>(1);
        indexToTable.put("t1", T3.T1_INFO.ident());
        indexToTable.put("t2", T3.T2_INFO.ident());

        Map<TableIdent, FetchSource> tableToFetchSource = new HashMap<>(2);

        ReferenceIdent referenceIdent1 = new ReferenceIdent(T3.T1_INFO.ident(), "a");
        Reference reference1 = new Reference(referenceIdent1,
            RowGranularity.DOC,
            StringType.INSTANCE,
            ColumnPolicy.STRICT,
            Reference.IndexType.NOT_ANALYZED,
            true);
        ReferenceIdent referenceIdent2 = new ReferenceIdent(T3.T1_INFO.ident(), "x");
        Reference reference2 = new Reference(referenceIdent2,
            RowGranularity.DOC,
            LongType.INSTANCE,
            ColumnPolicy.STRICT,
            Reference.IndexType.NOT_ANALYZED,
            true);
        FetchSource fetchSource = new FetchSource(Collections.<Reference>emptyList(),
            Collections.singletonList(new InputColumn(0)),
            Arrays.asList(reference1, reference2));
        tableToFetchSource.put(T3.T1_INFO.ident(), fetchSource);

        referenceIdent1 = new ReferenceIdent(T3.T2_INFO.ident(), "b");
        reference1 = new Reference(referenceIdent1,
            RowGranularity.DOC,
            StringType.INSTANCE,
            ColumnPolicy.STRICT,
            Reference.IndexType.NOT_ANALYZED,
            true);
        referenceIdent2 = new ReferenceIdent(T3.T2_INFO.ident(), "y");
        reference2 = new Reference(referenceIdent2,
            RowGranularity.DOC,
            LongType.INSTANCE,
            ColumnPolicy.STRICT,
            Reference.IndexType.NOT_ANALYZED,
            true);
        fetchSource = new FetchSource(Collections.<Reference>emptyList(),
            Collections.singletonList(new InputColumn(0)),
            Arrays.asList(reference1, reference2));
        tableToFetchSource.put(T3.T2_INFO.ident(), fetchSource);

        return new FetchProjectorContext(
            tableToFetchSource,
            nodeToReaderIds,
            readerIndices,
            indexToTable
        );
    }

    private List<Symbol> buildOutputSymbols() {
        List<Symbol> outputSymbols = new ArrayList<>(1);

        InputColumn inputColumn = new InputColumn(0);
        ReferenceIdent referenceIdent = new ReferenceIdent(T3.T1_INFO.ident(), "x");
        Reference reference = new Reference(referenceIdent,
            RowGranularity.DOC,
            LongType.INSTANCE,
            ColumnPolicy.STRICT,
            Reference.IndexType.NOT_ANALYZED,
            true);

        outputSymbols.add(new FetchReference(inputColumn, reference));
        return outputSymbols;
    }

    private List<Symbol> buildOutputSymbolsMultipleSources() {
        List<Symbol> outputSymbols = new ArrayList<>(2);

        InputColumn inputColumn = new InputColumn(0);
        ReferenceIdent referenceIdent = new ReferenceIdent(T3.T1_INFO.ident(), "a");
        Reference reference = new Reference(referenceIdent,
            RowGranularity.DOC,
            StringType.INSTANCE,
            ColumnPolicy.STRICT,
            Reference.IndexType.NOT_ANALYZED,
            true);
        outputSymbols.add(new FetchReference(inputColumn, reference));
        inputColumn = new InputColumn(1);
        referenceIdent = new ReferenceIdent(T3.T1_INFO.ident(), "x");
        reference = new Reference(referenceIdent,
            RowGranularity.DOC,
            LongType.INSTANCE,
            ColumnPolicy.STRICT,
            Reference.IndexType.NOT_ANALYZED,
            true);
        outputSymbols.add(new FetchReference(inputColumn, reference));

        return outputSymbols;
    }

    private static class DummyFetchOperation implements FetchOperation {

        int numFetches = 0;

        @Override
        public ListenableFuture<IntObjectMap<? extends Bucket>> fetch(String nodeId, IntObjectMap<? extends IntContainer> toFetch, boolean closeContext) {
            numFetches++;
            IntObjectHashMap<Bucket> readerToBuckets = new IntObjectHashMap<>();
            for (IntObjectCursor<? extends IntContainer> cursor : toFetch) {
                List<Object[]> rows = new ArrayList<>();
                for (IntCursor docIdCursor : cursor.value) {
                    rows.add(new Object[]{docIdCursor.value});
                }
                readerToBuckets.put(cursor.key, new CollectionBucket(rows));
            }
            return Futures.<IntObjectMap<? extends Bucket>>immediateFuture(readerToBuckets);
        }
    }

    private static Iterable<Row> rowRangeMultipleColumns() {
        return new Iterable<Row>() {

            @Override
            public Iterator<Row> iterator() {
                return new Iterator<Row>() {

                    private static final int NUMBER_OF_ROWS = 20;

                    private Object[] columns = new Object[3];
                    private RowN sharedRow = new RowN(columns);
                    private long i = 0;
                    private long step = 1;

                    @Override
                    public boolean hasNext() {
                        return i < NUMBER_OF_ROWS;
                    }

                    @Override
                    public Row next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException("Iterator exhausted");
                        }
                        columns[0] = String.valueOf(i);
                        columns[1] = i;
                        columns[2] = 100 + i;

                        i += step;
                        return sharedRow;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException("Remove not supported");
                    }
                };
            }
        };
    }
}

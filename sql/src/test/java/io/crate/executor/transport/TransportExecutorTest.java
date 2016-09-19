/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.executor.transport;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.DynamicReference;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.StmtCtx;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.*;
import io.crate.planner.node.management.KillPlan;
import io.crate.planner.projection.Projection;
import io.crate.testing.CollectingRowReceiver;
import org.elasticsearch.cluster.routing.Preference;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.crate.testing.TestingHelpers.isRow;
import static java.util.Arrays.asList;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;

public class TransportExecutorTest extends BaseTransportExecutorTest {

    @Test
    public void testESGetTask() throws Exception {
        setup.setUpCharacters();

        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        Planner.Context ctx = newPlannerContext();
        Plan plan = newGetNode("characters", outputs, "2", ctx.nextExecutionPhaseId());

        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        executor.execute(plan, rowReceiver);
        assertThat(rowReceiver.result(), contains(isRow(2, "Ford")));
    }

    @Test
    public void testESGetTaskWithDynamicReference() throws Exception {
        setup.setUpCharacters();

        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, new DynamicReference(
                new ReferenceIdent(new TableIdent(null, "characters"), "foo"), RowGranularity.DOC));
        Planner.Context ctx = newPlannerContext();
        Plan plan = newGetNode("characters", outputs, "2", ctx.nextExecutionPhaseId());
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        executor.execute(plan, rowReceiver);
        assertThat(rowReceiver.result(), contains(isRow(2, null)));
    }

    @Test
    public void testESMultiGet() throws Exception {
        setup.setUpCharacters();
        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        Planner.Context ctx = newPlannerContext();
        Plan plan = newGetNode("characters", outputs, asList("1", "2"), ctx.nextExecutionPhaseId());
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        executor.execute(plan, rowReceiver);
        assertThat(rowReceiver.result().size(), is(2));
    }

    @Test
    public void testKillTask() throws Exception {
        CollectingRowReceiver downstream = new CollectingRowReceiver();
        KillPlan plan = new KillPlan(UUID.randomUUID());
        executor.execute(plan, downstream);
        downstream.resultFuture().get(5, TimeUnit.SECONDS);
    }

    @Test
//    @ClusterScope()
    public void testUnionPlan() throws Exception {
        setup.setUpCharacters();

        Collection<Plan> childNodes = new ArrayList<>();
        Planner.Context plannerContext = newPlannerContext();

        TableInfo tableInfo = docSchemaInfo.getTableInfo("characters");
        assert tableInfo != null;
//        Reference uidReference = new Reference(
//            new ReferenceIdent(tableInfo.ident(), "_uid"), RowGranularity.DOC, DataTypes.STRING);

//        int executionNodeId = plannerContext.nextExecutionPhaseId();

        // 1st collect and merge nodes
        RoutedCollectPhase collectPhase1 = new RoutedCollectPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "collect1",
            plannerContext.allocateRouting(tableInfo, WhereClause.MATCH_ALL, Preference.PRIMARY.type()),
            RowGranularity.DOC,
            ImmutableList.<Symbol>of(),
            ImmutableList.<Projection>of(),
            WhereClause.MATCH_ALL,
            DistributionInfo.DEFAULT_MODULO
        );
        MergePhase mergeNode1 = MergePhase.localMerge(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            ImmutableList.<Projection>of(),
            collectPhase1.executionNodes().size(),
            collectPhase1.outputTypes());
        childNodes.add(new CollectAndMerge(collectPhase1, mergeNode1));

        // 2nd collect and merge nodes
        RoutedCollectPhase collectPhase2 = new RoutedCollectPhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "collect2",
            plannerContext.allocateRouting(tableInfo, WhereClause.MATCH_ALL, Preference.PRIMARY.type()),
            RowGranularity.DOC,
            ImmutableList.<Symbol>of(),
            ImmutableList.<Projection>of(),
            WhereClause.MATCH_ALL,
            DistributionInfo.DEFAULT_MODULO
        );
        MergePhase mergeNode2 = MergePhase.localMerge(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            ImmutableList.<Projection>of(),
            collectPhase2.executionNodes().size(),
            collectPhase2.outputTypes());
        childNodes.add(new CollectAndMerge(collectPhase2, mergeNode2));

        UnionPhase unionPhase = new UnionPhase(plannerContext.jobId(),
                                               plannerContext.nextExecutionPhaseId(),
//                                                executionNodeId,
                                               "union",
                                               ImmutableList.<Projection>of(),
                                               Arrays.asList(mergeNode1, mergeNode2),
                                               ImmutableSet.<String>of());

        UnionPlan unionPlan = new UnionPlan(unionPhase, childNodes);
        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        executor.execute(unionPlan, rowReceiver);
        assertThat(rowReceiver.result().size(), is(2));
    }

    protected Planner.Context newPlannerContext() {
        return new Planner.Context(clusterService(), UUID.randomUUID(), null, new StmtCtx(), 0, 0);
    }
}

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

package io.crate.planner.node.dql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.is;

public class UnionPhaseTest extends CrateUnitTest {

    @Test
    public void testSerialization() throws Exception {
        TopNProjection topNProjection = new TopNProjection(10, 0);
        UUID jobId = UUID.randomUUID();
        Collection<MergePhase> mergePhases = new ArrayList<>(2);
        MergePhase mp1 = new MergePhase(jobId, 2, "merge", 1,
                ImmutableList.<DataType>of(DataTypes.STRING),
                ImmutableList.<Projection>of(),
                DistributionInfo.DEFAULT_BROADCAST);
        mergePhases.add(mp1);
        MergePhase mp2 = new MergePhase(jobId, 3, "merge", 1,
                ImmutableList.<DataType>of(DataTypes.STRING),
                ImmutableList.<Projection>of(),
                DistributionInfo.DEFAULT_BROADCAST);
        mergePhases.add(mp2);
        UnionPhase phase1 = new UnionPhase(jobId,
                                           1,
                                           "unionAll",
                                           ImmutableList.<Projection>of(topNProjection),
                                           mergePhases,
                                           Sets.newHashSet("node1", "node2"));

        BytesStreamOutput output = new BytesStreamOutput();
        phase1.writeTo(output);

        StreamInput input = StreamInput.wrap(output.bytes());
        UnionPhase phase2 = new UnionPhase();
        phase2.readFrom(input);

        assertThat(phase1.executionNodes(), Is.is(phase2.executionNodes()));
        assertThat(phase1.mergePhases(), Is.is(phase2.mergePhases()));
        assertThat(phase1.jobId(), Is.is(phase2.jobId()));
        assertThat(phase1.name(), is(phase2.name()));
        assertThat(phase1.outputTypes(), is(phase2.outputTypes()));
    }
}

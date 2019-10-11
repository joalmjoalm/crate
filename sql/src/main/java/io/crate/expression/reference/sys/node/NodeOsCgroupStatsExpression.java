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

package io.crate.expression.reference.sys.node;

import org.elasticsearch.monitor.os.OsStats;

import java.util.function.Function;

public class NodeOsCgroupStatsExpression extends NestedNodeStatsExpression {

    private static final String CPUACCT = "cpuacct";
    private static final String CPU = "cpu";
    private static final String MEM = "mem";

    NodeOsCgroupStatsExpression() {
        childImplementations.put(CPUACCT, new NodeOsCgroupCpuAcctStatsExpression());
        childImplementations.put(CPU, new NodeOsCgroupCpuStatsExpression());
        childImplementations.put(MEM, new NodeOsCgroupMemStatsExpression());
    }

    @Override
    public void setNextRow(NodeStatsContext nodeStatsContext) {
        value = null;
        if (nodeStatsContext.isComplete()) {
            OsStats.Cgroup cgroup = nodeStatsContext.extendedOsStats().osStats().getCgroup();
            if (cgroup != null) {
                super.setNextRow(nodeStatsContext);
            }
        }
    }

    private static class NodeOsCgroupCpuAcctStatsExpression extends NestedNodeStatsExpression {

        private static final String CONTROL_GROUP = "control_group";
        private static final String USAGE_NANOS = "usage_nanos";

        NodeOsCgroupCpuAcctStatsExpression() {
            childImplementations.put(CONTROL_GROUP, CgroupExpression.forAttribute(OsStats.Cgroup::getCpuAcctControlGroup));
            childImplementations.put(USAGE_NANOS, CgroupExpression.forAttribute(OsStats.Cgroup::getCpuAcctUsageNanos));
        }

        @Override
        public void setNextRow(NodeStatsContext nodeStatsContext) {
            value = null;
            if (nodeStatsContext.isComplete()) {
                OsStats.Cgroup cgroup = nodeStatsContext.extendedOsStats().osStats().getCgroup();
                if (cgroup != null) {
                    super.setNextRow(nodeStatsContext);
                }
            }
        }
    }

    private class NodeOsCgroupCpuStatsExpression extends NestedNodeStatsExpression {

        private static final String CONTROL_GROUP = "control_group";
        private static final String CFS_PERIOD_MICROS = "cfs_period_micros";
        private static final String CFS_QUOTA_MICROS = "cfs_quota_micros";
        private static final String NUM_ELAPSED_PERIODS = "num_elapsed_periods";
        private static final String NUM_TIMES_THROTTLED = "num_times_throttled";
        private static final String TIME_THROTTLED_NANOS = "time_throttled_nanos";

        NodeOsCgroupCpuStatsExpression() {
            childImplementations.put(CONTROL_GROUP, CgroupExpression.forAttribute(OsStats.Cgroup::getCpuControlGroup));
            childImplementations.put(CFS_PERIOD_MICROS, CgroupExpression.forAttribute(OsStats.Cgroup::getCpuCfsPeriodMicros));
            childImplementations.put(CFS_QUOTA_MICROS, CgroupExpression.forAttribute(OsStats.Cgroup::getCpuCfsQuotaMicros));
            childImplementations.put(NUM_ELAPSED_PERIODS, CgroupExpression.forAttribute(
                ((Function<OsStats.Cgroup, OsStats.Cgroup.CpuStat>) OsStats.Cgroup::getCpuStat)
                    .andThen(OsStats.Cgroup.CpuStat::getNumberOfElapsedPeriods)));
            childImplementations.put(NUM_TIMES_THROTTLED, CgroupExpression.forAttribute(
                ((Function<OsStats.Cgroup, OsStats.Cgroup.CpuStat>) OsStats.Cgroup::getCpuStat)
                    .andThen(OsStats.Cgroup.CpuStat::getNumberOfTimesThrottled)));
            childImplementations.put(TIME_THROTTLED_NANOS, CgroupExpression.forAttribute(
                ((Function<OsStats.Cgroup, OsStats.Cgroup.CpuStat>) OsStats.Cgroup::getCpuStat)
                    .andThen(OsStats.Cgroup.CpuStat::getTimeThrottledNanos)));
        }

        @Override
        public void setNextRow(NodeStatsContext nodeStatsContext) {
            value = null;
            if (nodeStatsContext.isComplete()) {
                OsStats.Cgroup cgroup = cgroup(nodeStatsContext);
                if (cgroup != null) {
                    super.setNextRow(nodeStatsContext);
                }
            }
        }
    }

    private static OsStats.Cgroup cgroup(NodeStatsContext row) {
        return row.extendedOsStats().osStats().getCgroup();
    }

    private class NodeOsCgroupMemStatsExpression extends NestedNodeStatsExpression {

        private static final String CONTROL_GROUP = "control_group";
        private static final String LIMIT_BYTES = "limit_bytes";
        private static final String USAGE_BYTES = "usage_bytes";

        NodeOsCgroupMemStatsExpression() {
            childImplementations.put(CONTROL_GROUP, new SimpleNodeStatsExpression<String>() {
                @Override
                public String innerValue(NodeStatsContext nodeStatsContext) {
                    if (nodeStatsContext.isComplete()) {
                        OsStats.Cgroup cgroup = cgroup(nodeStatsContext);
                        if (cgroup != null) {
                            return cgroup.getMemoryControlGroup();
                        }
                    }
                    return null;
                }
            });
            childImplementations.put(LIMIT_BYTES, new SimpleNodeStatsExpression<String>() {
                @Override
                public String innerValue(NodeStatsContext nodeStatsContext) {
                    if (nodeStatsContext.isComplete()) {
                        OsStats.Cgroup cgroup = cgroup(nodeStatsContext);
                        if (cgroup != null) {
                            return cgroup.getMemoryLimitInBytes();
                        }
                    }
                    return null;
                }
            });
            childImplementations.put(USAGE_BYTES, new SimpleNodeStatsExpression<String>() {
                @Override
                public String innerValue(NodeStatsContext nodeStatsContext) {
                    if (nodeStatsContext.isComplete()) {
                        OsStats.Cgroup cgroup = cgroup(nodeStatsContext);
                        if (cgroup != null) {
                            return cgroup.getMemoryUsageInBytes();
                        }
                    }
                    return null;
                }
            });
        }

        @Override
        public void setNextRow(NodeStatsContext nodeStatsContext) {
            value = null;
            if (nodeStatsContext.isComplete()) {
                OsStats.Cgroup cgroup = cgroup(nodeStatsContext);
                if (cgroup != null) {
                    super.setNextRow(nodeStatsContext);
                }
            }
        }
    }
}

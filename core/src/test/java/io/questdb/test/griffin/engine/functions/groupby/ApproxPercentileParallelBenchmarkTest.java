/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.griffin.engine.functions.groupby;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.groupby.ApproxPercentileLongGroupByFunctionFactory;
import io.questdb.mp.WorkerPool;
import io.questdb.std.Misc;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.io.FileWriter;
import java.io.PrintWriter;

public class ApproxPercentileParallelBenchmarkTest extends AbstractCairoTest {

    private static final int WARMUP_ITERATIONS = 3;
    private static final int MEASUREMENT_ITERATIONS = 5;
    private static final int TOTAL_ROWS = 10_000_000;
    private static final int GROUPS = 100;

    @Test
    public void runBenchmark() throws Exception {
        int[] workerCounts = {1, 2, 4, 8};
        int[] partitionCounts = {8, 16, 32, 64};

        try (PrintWriter writer = new PrintWriter(new FileWriter("benchmark_results.txt"))) {
            writer.println("Partitions | Workers | master(ms) | branch(ms) | Speedup");
            writer.println("-----------|---------|------------|------------|--------");

            for (int partitions : partitionCounts) {
                for (int workers : workerCounts) {
                    ApproxPercentileLongGroupByFunctionFactory.useParallel = false;
                    double masterMs = runBenchmark(workers, partitions);

                    ApproxPercentileLongGroupByFunctionFactory.useParallel = true;
                    double branchMs = runBenchmark(workers, partitions);

                    writer.printf("%10d | %7d | %10.2f | %10.2f | %.2fx%n",
                            partitions, workers, masterMs, branchMs, masterMs / branchMs);
                }
            }
        }
    }

    private double runBenchmark(int workerCount, int partitions) throws Exception {
        WorkerPool pool = new WorkerPool(() -> workerCount);
        CairoEngine engine = new CairoEngine(configuration);
        SqlExecutionContext sqlExecutionContext = TestUtils.createSqlExecutionCtx(engine, workerCount);
        try {
            TestUtils.setupWorkerPool(pool, engine);
            pool.start(LOG);

            engine.execute(
                    "CREATE TABLE bench_test (" +
                            "  ts TIMESTAMP," +
                            "  grp SYMBOL," +
                            "  value LONG" +
                            ") TIMESTAMP(ts) PARTITION BY HOUR",
                    sqlExecutionContext);

            engine.execute(
                    "INSERT INTO bench_test " +
                            "SELECT dateadd('h', (x % " + partitions + ")::int, '2024-01-01'::timestamp), " +
                            "       'group_' || (x % " + GROUPS + "), " +
                            "       abs(rnd_long() % 1000000) " +
                            "FROM long_sequence(" + TOTAL_ROWS + ")",
                    sqlExecutionContext);

            String query = "SELECT grp, approx_percentile(value, 0.5) AS p50, " +
                    "approx_percentile(value, 0.99) AS p99 " +
                    "FROM bench_test GROUP BY grp";

            long minTime = Long.MAX_VALUE;
            try (RecordCursorFactory factory = engine.select(query, sqlExecutionContext)) {
                for (int i = 0; i < WARMUP_ITERATIONS; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        while (cursor.hasNext()) {}
                    }
                }

                for (int i = 0; i < MEASUREMENT_ITERATIONS; i++) {
                    try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
                        long start = System.nanoTime();
                        while (cursor.hasNext()) {}
                        minTime = Math.min(minTime, System.nanoTime() - start);
                    }
                }
            }

            engine.execute("DROP TABLE bench_test", sqlExecutionContext);
            return minTime / 1_000_000.0;
        } finally {
            pool.halt();
            Misc.free(engine);
        }
    }

}

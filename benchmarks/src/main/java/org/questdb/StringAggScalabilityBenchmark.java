/*+*****************************************************************************
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

package org.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.Misc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public class StringAggScalabilityBenchmark {

    private static final int GROUP_COUNT = Integer.getInteger("groupCount", 10);
    private static final int MEASUREMENT_ITERATIONS = Integer.getInteger("measurementIterations", 5);
    private static final int ROW_COUNT = Integer.getInteger("rowCount", 1_000_000);
    private static final int WARMUP_ITERATIONS = Integer.getInteger("warmupIterations", 2);
    private static final int[] WORKER_COUNTS = {1, 2, 4, 8};

    public static void main(String[] args) throws Exception {
        System.out.printf("PARAMS rowCount=%d groupCount=%d%n", ROW_COUNT, GROUP_COUNT);
        System.out.printf("%8s %10s %10s%n", "workers", "ms", "speedup");
        double baselineMs = -1;
        for (int workers : WORKER_COUNTS) {
            double timeMs = run(workers);
            if (baselineMs < 0) {
                baselineMs = timeMs;
            }
            System.out.printf("RESULT rowCount=%d groupCount=%d workers=%d ms=%.2f speedup=%.2fx%n", ROW_COUNT, GROUP_COUNT, workers, timeMs, baselineMs / timeMs);
        }
    }

    private static double run(int workers) throws Exception {
        Path tempRoot = Files.createTempDirectory("stringaggscalebench-");
        CairoConfiguration configuration = new DefaultCairoConfiguration(tempRoot.toString()) {
            @Override
            public int getStrFunctionMaxBufferLength() {
                return Integer.MAX_VALUE;
            }
        };
        CairoEngine engine = new CairoEngine(configuration);
        WorkerPool pool = null;
        try {
            if (workers > 1) {
                pool = new WorkerPool(() -> workers);
                WorkerPoolUtils.setupQueryJobs(pool, engine);
                pool.start();
            }
            SqlExecutionContext ctx = new SqlExecutionContextImpl(engine, workers)
                    .with(configuration.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
            ctx.setParallelGroupByEnabled(workers > 1);

            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                engine.execute("CREATE TABLE tab (k SYMBOL, s STRING, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", ctx);
                engine.execute(
                        "INSERT INTO tab SELECT (x % " + GROUP_COUNT + ")::SYMBOL, rnd_str(6, 10, 0), " +
                                "timestamp_sequence(0, 100000) FROM long_sequence(" + ROW_COUNT + ")",
                        ctx
                );
                try (RecordCursorFactory factory = compiler.compile("SELECT k, string_agg(s, ',') FROM tab", ctx).getRecordCursorFactory()) {
                    List<Double> times = new ArrayList<>();
                    for (int i = 0; i < WARMUP_ITERATIONS + MEASUREMENT_ITERATIONS; i++) {
                        long t0 = System.nanoTime();
                        try (RecordCursor cursor = factory.getCursor(ctx)) {
                            Record record = cursor.getRecord();
                            while (cursor.hasNext()) {
                                record.getStrA(1);
                            }
                        }
                        double ms = (System.nanoTime() - t0) / 1_000_000.0;
                        if (i >= WARMUP_ITERATIONS) {
                            times.add(ms);
                        }
                    }
                    times.sort(Double::compareTo);
                    return times.get(times.size() / 2);
                }
            }
        } finally {
            if (pool != null) {
                pool.halt();
            }
            Misc.free(engine);
            try (Stream<Path> stream = Files.walk(tempRoot)) {
                stream.sorted(Comparator.reverseOrder()).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (Exception ignore) {
                    }
                });
            }
        }
    }
}

package com.tinkermic.benchmark.jmh;

import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Base class for all TinkerPop OpenJDK JMH benchmarks. Based upon Netty's approach to running JMH benchmarks
 * from JUnit.
 *
 * @see <a href="http://netty.io/wiki/microbenchmarks.html">http://netty.io/wiki/microbenchmarks.html</a>
 */
@Warmup(iterations = AbstractBenchmarkBase.DEFAULT_WARMUP_ITERATIONS)
@Measurement(iterations = AbstractBenchmarkBase.DEFAULT_MEASURE_ITERATIONS)
@Fork(AbstractBenchmarkBase.DEFAULT_FORKS)
public abstract class AbstractBenchmarkBase {

    protected static final int DEFAULT_WARMUP_ITERATIONS = 3;
    protected static final int DEFAULT_MEASURE_ITERATIONS = 3;
    protected static final int DEFAULT_FORKS = 1;
    protected static final Mode DEFAULT_MODE = Mode.Throughput;

    protected static final String DEFAULT_BENCHMARK_DIRECTORY = "./benchmarks/";

    protected static final String[] JVM_ARGS = {
            "-Xms2g", "-Xmx2g", "-XX:+UseParNewGC", "-XX:+UseConcMarkSweepGC", "-XX:+CMSParallelRemarkEnabled", "-XX:+AlwaysPreTouch"
    };

    @Test
    public void run() throws Exception {
        final String className = getClass().getSimpleName();

        final ChainedOptionsBuilder runnerOptions = new OptionsBuilder()
                .include(".*" + className + ".*")
                .jvmArgs(JVM_ARGS);

        if (getWarmupIterations() > 0) {
            runnerOptions.warmupIterations(getWarmupIterations());
        }

        if (getMeasureIterations() > 0) {
            runnerOptions.measurementIterations(getMeasureIterations());
        }

        if (getForks() > 0) {
            runnerOptions.forks(getForks());
        }

        runnerOptions.mode(getMode());

        if (getReportDir() != null) {
            final String dtmStr = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
            final String filePath = getReportDir() + className + "-" + dtmStr + ".json";
            final File file = new File(filePath);
            if (file.exists()) {
                file.delete();
            } else {
                file.getParentFile().mkdirs();
                file.createNewFile();
            }

            runnerOptions.resultFormat(ResultFormatType.JSON);
            runnerOptions.result(filePath);
        }

        new Runner(runnerOptions.build()).run();
    }

    protected int getWarmupIterations() {
        return getIntProperty("warmupIterations", DEFAULT_WARMUP_ITERATIONS);
    }

    protected int getMeasureIterations() {
        return getIntProperty("measureIterations", DEFAULT_MEASURE_ITERATIONS);
    }

    protected int getForks() {
        return getIntProperty("forks", DEFAULT_FORKS);
    }

    protected Mode getMode() {
        return Mode.valueOf(System.getProperty("mode", DEFAULT_MODE.toString()));
    }

    protected String getReportDir() {
        return System.getProperty("benchmark.dir", DEFAULT_BENCHMARK_DIRECTORY);
    }

    private int getIntProperty(final String propertyName, final int defaultValue) {
        final String propertyValue = System.getProperty(propertyName);
        if (propertyValue == null) {
            return defaultValue;
        }
        return Integer.valueOf(propertyValue);
    }
}
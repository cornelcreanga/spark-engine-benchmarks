package dev.sparkbench.benchmarks;

import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.*;

import java.util.*;

final class TaskCounters extends SparkListener {
    private long tasks,
            cpuNs,
            runMs,
            gcMs,
            memorySpill,
            diskSpill,
            shuffleRead,
            shuffleWrite,
            input,
            output,
            peakTaskMemory;
    volatile long finished;

    public double finishedSeconds(long start) {
        return (finished - start) / 1e9;
    }

    @Override
    public synchronized void onTaskEnd(SparkListenerTaskEnd event) {
        TaskMetrics m = event.taskMetrics();
        if (m == null) {
            return;
        }
        tasks++;
        cpuNs += m.executorCpuTime();
        runMs += m.executorRunTime();
        gcMs += m.jvmGCTime();
        memorySpill += m.memoryBytesSpilled();
        diskSpill += m.diskBytesSpilled();
        shuffleRead += m.shuffleReadMetrics().totalBytesRead();
        shuffleWrite += m.shuffleWriteMetrics().bytesWritten();
        input += m.inputMetrics().bytesRead();
        output += m.outputMetrics().bytesWritten();
        peakTaskMemory = Math.max(peakTaskMemory, m.peakExecutionMemory());
    }

    public synchronized Map<String, Object> values() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("taskAttempts", tasks);
        m.put("executorCpuNs", cpuNs);
        m.put("executorRunMs", runMs);
        m.put("jvmGcMs", gcMs);
        m.put("memoryBytesSpilled", memorySpill);
        m.put("diskBytesSpilled", diskSpill);
        m.put("shuffleReadBytes", shuffleRead);
        m.put("shuffleWriteBytes", shuffleWrite);
        m.put("inputBytes", input);
        m.put("outputBytes", output);
        m.put("maxTaskPeakExecutionMemoryBytes", peakTaskMemory);
        return m;
    }
}

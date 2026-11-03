package dev.sparkbench.runner;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

final class Commands {
    static String run(List<String> args) throws Exception {
        return run(args, 600);
    }

    static String run(List<String> args, long seconds) throws Exception {
        return execute(args, seconds, true);
    }

    static String stdout(List<String> args, long seconds) throws Exception {
        return execute(args, seconds, false);
    }

    private static String execute(List<String> args, long seconds, boolean mergeErrors)
            throws Exception {
        System.out.println("$ " + String.join(" ", args));
        Path output = Files.createTempFile("sparkbench-command-", ".log");
        Path errors = Files.createTempFile("sparkbench-command-errors-", ".log");
        try {
            Process p =
                    new ProcessBuilder(args)
                            .redirectErrorStream(mergeErrors)
                            .redirectError(errors.toFile())
                            .redirectOutput(output.toFile())
                            .start();
            if (!p.waitFor(seconds, TimeUnit.SECONDS)) {
                p.destroyForcibly();
                p.waitFor();
                throw new IOException("Command timed out: " + args);
            }
            String text = Files.readString(output);
            if (p.exitValue() != 0) {
                throw new IOException(
                        "Command exited "
                                + p.exitValue()
                                + ": "
                                + args
                                + "\n"
                                + text
                                + Files.readString(errors));
            }
            return text;
        } finally {
            Files.deleteIfExists(output);
            Files.deleteIfExists(errors);
        }
    }
}

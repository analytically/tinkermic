package com.tinkermic.benchmark;

import com.tinkermic.gremlin.BuildInfo;
import org.openjdk.jmh.runner.*;
import org.openjdk.jmh.runner.options.CommandLineOptionException;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.IOException;

/**
 * Benchmark entry point.
 */
public class Main {
    public static void main(String[] argv) throws RunnerException, IOException {
        System.out.println(new StringBuilder()
                .append("Running benchmark using ")
                .append(BuildInfo.name())
                .append(" ")
                .append(BuildInfo.version())
                .append(", built ")
                .append(BuildInfo.builtAtString())
                .append(" (git ")
                .append(BuildInfo.gitRevision())
                .append(")\n")
                .toString()
        );

        try {
            CommandLineOptions cmdOptions = new CommandLineOptions(argv);

            Runner runner = new Runner(cmdOptions);

            if (cmdOptions.shouldHelp()) {
                cmdOptions.showHelp();
                return;
            }

            if (cmdOptions.shouldList()) {
                runner.list();
                return;
            }

            if (cmdOptions.shouldListWithParams()) {
                runner.listWithParams(cmdOptions);
                return;
            }

            if (cmdOptions.shouldListProfilers()) {
                cmdOptions.listProfilers();
                return;
            }

            if (cmdOptions.shouldListResultFormats()) {
                cmdOptions.listResultFormats();
                return;
            }

            try {
                runner.run();
            } catch (NoBenchmarksException e) {
                System.err.println("No matching benchmarks. Miss-spelled regexp?");

                if (cmdOptions.verbosity().orElse(Defaults.VERBOSITY) != VerboseMode.EXTRA) {
                    System.err.println("Use " + VerboseMode.EXTRA + " verbose mode to debug the pattern matching.");
                } else {
                    runner.list();
                }
                System.exit(1);
            } catch (ProfilersFailedException e) {
                // This is not exactly an error, set non-zero exit code
                System.err.println(e.getMessage());
                System.exit(1);
            } catch (RunnerException e) {
                System.err.print("ERROR: ");
                e.printStackTrace(System.err);
                System.exit(1);
            }

        } catch (CommandLineOptionException e) {
            System.err.println("Error parsing command line:");
            System.err.println(" " + e.getMessage());
            System.exit(1);
        }
    }
}

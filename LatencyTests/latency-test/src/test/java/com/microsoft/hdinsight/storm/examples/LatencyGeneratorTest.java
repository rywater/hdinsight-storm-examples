package com.microsoft.hdinsight.storm.examples;

import org.HdrHistogram.Histogram;
import org.junit.Before;
import org.junit.Test;

/**
 * Sample class for experimenting with different latency values
 */
public class LatencyGeneratorTest {

  private Histogram histogram;

  @Before
  public void setUp() throws Exception {
    this.histogram = new Histogram(3);
  }

  @Test
  public void getNextWait() throws Exception {
    LatencyGenerator latencyGenerator = new LatencyGenerator(100, 100, 0, 0, .5);
    runTest(latencyGenerator, 1000000);
  }

  private void runTest(LatencyGenerator latencyGenerator, int iterations) {
    for (int i = 0; i < iterations; i++) {
      consumeResult(latencyGenerator.determineNextWait(i % 10));
    }
    printHistogram();
  }

  private void consumeResult(double result) {
    this.histogram.recordValue((long) result);
  }

  private void printHistogram() {
    this.histogram.outputPercentileDistribution(System.out, 1000.0);
  }

}
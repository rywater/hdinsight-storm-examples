package com.microsoft.hdinsight.storm.examples;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;
import java.util.Random;

/**
 * Add this bolt and drive the ops team crazy!
 */
public class LatencyBolt extends BaseBasicBolt {

  private static final long serialVersionUID = 2639024139110583847L;

  private static final Logger LOGGER = LoggerFactory.getLogger(LatencyBolt.class);

  private final LatencyGenerator latencyGenerator;

  public LatencyBolt(LatencyGenerator latencyGenerator) {
    this.latencyGenerator = Objects.requireNonNull(latencyGenerator);
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    try {
      long wait = getNextWait();
      LOGGER.info("Waiting for {}", wait);
      Thread.sleep(wait);
    } catch (InterruptedException e) {
      LOGGER.warn("Thread interrupted", e);
    }
    collector.emit(Collections.singletonList(input));
  }

  private long getNextWait() {
    return (long) this.latencyGenerator.getNextWait();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }
}

package com.microsoft.hdinsight.storm.examples;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class LatencyBolt extends BaseRichBolt {

  private static final long serialVersionUID = 2639024139110583847L;

  private static final Logger LOGGER = LoggerFactory.getLogger(LatencyBolt.class);

  private final LatencyGenerator latencyGenerator;

  private OutputCollector outputCollector;
  private int boltId;

  public LatencyBolt(LatencyGenerator latencyGenerator) {
    this.latencyGenerator = Objects.requireNonNull(latencyGenerator);
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.outputCollector = collector;
    this.boltId = context.getThisTaskId();
  }

  @Override
  public void execute(Tuple input) {
    try {
      long wait = this.latencyGenerator.determineNextWait(this.boltId);
      LOGGER.debug("Waiting for {}", wait);
      Thread.sleep(wait);
    } catch (InterruptedException e) {
      LOGGER.warn("Thread interrupted", e);
    }
    this.outputCollector.emit(input, Collections.singletonList(input));
    this.outputCollector.ack(input);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("message"));
  }
}

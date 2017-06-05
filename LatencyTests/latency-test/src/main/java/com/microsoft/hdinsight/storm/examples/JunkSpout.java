package com.microsoft.hdinsight.storm.examples;

import clojure.lang.IFn;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class JunkSpout extends BaseRichSpout {

  private static final Logger LOGGER = LoggerFactory.getLogger(JunkSpout.class);

  private final MessageGenerator messageGenerator;
  private final long waitTime;

  private SpoutOutputCollector collector;

  /**
   * @param messageGenerator provides random messages for emission from this spout
   * @param waitTime         set a deliberate wait time before each message emission
   */
  public JunkSpout(MessageGenerator messageGenerator, long waitTime) {
    this.messageGenerator = Objects.requireNonNull(messageGenerator);
    this.waitTime = waitTime;
  }

  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    this.collector = collector;
  }

  public void nextTuple() {
    try {
      if (waitTime > 0) {
        Thread.sleep(waitTime);
      }
      this.collector.emit(new Values(this.messageGenerator.getNextMessage()), UUID.randomUUID());
    } catch (InterruptedException e) {
      LOGGER.error(e.getMessage(), e);
    }
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("message"));
  }

}

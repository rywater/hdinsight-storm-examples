package com.microsoft.hdinsight.storm.examples;

import java.io.Serializable;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

public class LatencyGenerator implements Serializable {

  private static final long serialVersionUID = -357869071120987962L;
  private final Random random;
  private final double min;
  private final double max;
  private final double skew;
  private final double bias;
  private final double impact;
  private final List<Integer> boltIds;
  private final List<Integer> impactedBolts;


  /**
   * @param min    The minimum skew value possible
   * @param max    The maximum skew value possible
   * @param skew   How tightly to bound the skew, a lower value gives a more even distribution
   * @param bias   Tendency towards min, max, or midpoint. Positive values towards max, negatives towards the min.
   * @param impact Percentage of bolts to impact. 1 for all, 0 for none. Ex. 10 bolt instances with a .5 impact will
   *               apply latency to only 5 of the bolts
   */
  public LatencyGenerator(double min, double max, double skew, double bias, double impact) {
    this.min = min;
    this.max = max;
    this.skew = skew;
    this.bias = bias;
    this.impact = impact;
    this.boltIds = new CopyOnWriteArrayList<>();
    this.impactedBolts = new CopyOnWriteArrayList<>();
    this.random = new Random();
  }

  public long determineNextWait(int boltId) {
    long wait = 0;
    if (shouldImpact(boltId)) {
      double range = max - min;
      double mid = min + range / 2.0;
      double unitGaussian = random.nextGaussian();
      double biasFactor = Math.exp(bias);
      wait = (long) (mid + (range * (biasFactor / (biasFactor + Math.exp(-unitGaussian / skew)) - 0.5)));
    }
    return wait;
  }

  private boolean shouldImpact(int boltId) {
    if (!this.boltIds.contains(boltId)) {
      this.boltIds.add(boltId);
      this.impactedBolts.clear();
      this.impactedBolts.addAll(this.boltIds.subList(0, (int) (this.boltIds.size() * impact)));
    }
    return this.impactedBolts.contains(boltId);
  }
}

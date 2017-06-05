package com.microsoft.hdinsight.storm.examples;

import java.io.Serializable;
import java.util.Random;

public class LatencyGenerator implements Serializable {

  private static final long serialVersionUID = -357869071120987962L;
  private final Random random;
  private final double min;
  private final double max;
  private final double skew;
  private final double bias;


  /**
   * @param min  The minimum skew value possible
   * @param max  The maximum skew value possible
   * @param skew How tightly to bound the skew, a lower value gives a more even distribution
   * @param bias Tendency towards min, max, or midpoint. Positive values towards max, negatives towards the min.
   */
  public LatencyGenerator(double min, double max, double skew, double bias) {
    this.min = min;
    this.max = max;
    this.skew = skew;
    this.bias = bias;
    this.random = new Random();
  }

  public double getNextWait() {
    double range = max - min;
    double mid = min + range / 2.0;
    double unitGaussian = random.nextGaussian();
    double biasFactor = Math.exp(bias);
    return (long) (mid + (range * (biasFactor / (biasFactor + Math.exp(-unitGaussian / skew)) - 0.5)));
  }
}

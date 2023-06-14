package statistics.collector;

import java.util.concurrent.TimeUnit;

class ExtTimeWindowQuantiles {

  private final ExtCKMSQuantiles.Quantile[] quantiles;
  private final ExtCKMSQuantiles[] ringBuffer;
  private int currentBucket;
  private long lastRotateTimestampMillis;
  private final long durationBetweenRotatesMillis;

  public ExtTimeWindowQuantiles(ExtCKMSQuantiles.Quantile[] quantiles, long maxAgeSeconds, int ageBuckets) {
    this.quantiles = quantiles;
    this.ringBuffer = new ExtCKMSQuantiles[ageBuckets];
    for (int i = 0; i < ageBuckets; i++) {
      this.ringBuffer[i] = new ExtCKMSQuantiles(quantiles);
    }
    this.currentBucket = 0;
    this.lastRotateTimestampMillis = System.currentTimeMillis();
    this.durationBetweenRotatesMillis = TimeUnit.SECONDS.toMillis(maxAgeSeconds) / ageBuckets;
  }

  public synchronized double get(double q) {
    ExtCKMSQuantiles currentBucket = rotate();
    return currentBucket.get(q);
  }

  public synchronized void insert(double value) {
    rotate();
    for (ExtCKMSQuantiles ckmsQuantiles : ringBuffer) {
      ckmsQuantiles.insert(value);
    }
  }

  private ExtCKMSQuantiles rotate() {
    long timeSinceLastRotateMillis = System.currentTimeMillis() - lastRotateTimestampMillis;
    while (timeSinceLastRotateMillis > durationBetweenRotatesMillis) {
      ringBuffer[currentBucket] = new ExtCKMSQuantiles(quantiles);
      if (++currentBucket >= ringBuffer.length) {
        currentBucket = 0;
      }
      timeSinceLastRotateMillis -= durationBetweenRotatesMillis;
      lastRotateTimestampMillis += durationBetweenRotatesMillis;
    }
    return ringBuffer[currentBucket];
  }
}

package statistics.collector;

import io.prometheus.client.Collector;

public class ExtSimpleTimer {
  private final long start;
  static TimeProvider defaultTimeProvider = new TimeProvider();
  private final TimeProvider timeProvider;

  static class TimeProvider {
    long nanoTime() {
      return System.nanoTime();
    }
  }

  ExtSimpleTimer(TimeProvider timeProvider) {
    this.timeProvider = timeProvider;
    start = timeProvider.nanoTime();
  }

  public ExtSimpleTimer() {
    this(defaultTimeProvider);
  }

  public double elapsedSeconds() {
    return elapsedSecondsFromNanos(start, timeProvider.nanoTime());
  }

  public static double elapsedSecondsFromNanos(long startNanos, long endNanos) {
      return (endNanos - startNanos) / Collector.NANOSECONDS_PER_SECOND;
  }
}

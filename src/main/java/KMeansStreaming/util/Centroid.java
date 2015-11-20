package KMeansStreaming.util;

/**
 * Created by ken on 11/18/2015.
 */
public class Centroid
{
  public double[] feature;
  public int counts;

  public Centroid(double[] feature) {
    this.feature = feature;
    counts = 0;
  }
}
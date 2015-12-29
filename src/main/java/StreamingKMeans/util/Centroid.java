package StreamingKMeans.util;

/**
 * Created by ken on 11/18/2015.
 */
public class Centroid
{
  public Feature feature;
  public int counts;

  public Centroid(Feature feature) {
    this.feature = feature;
    counts = 0;
  }
}
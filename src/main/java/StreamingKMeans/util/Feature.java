package StreamingKMeans.util;

/**
 * Created by ken on 12/28/2015.
 */
public class Feature {
  public double[] value;
  public String location;
  public long createdTime;

  public Feature(double[] val, String loc)
  {
    value = val;
    location = loc;
    createdTime = System.currentTimeMillis();
  }

  public Feature(double[] val, String loc, long time)
  {
    value = val;
    location = loc;
    createdTime = time;
  }
}

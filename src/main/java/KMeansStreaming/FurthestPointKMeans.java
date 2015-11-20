package KMeansStreaming;

import KMeansStreaming.util.Centroid;
import KMeansStreaming.util.MathUtil;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Created by ken on 11/16/2015.
 */
@SuppressWarnings("Duplicates")
public class FurthestPointKMeans implements Clusterer, Serializable {

  private Integer nbCluster;
  private double radius;
  private double error;
  private double errorRadius;
  private ArrayList<double[]> features = new ArrayList<double[]>();
  private ArrayList<Centroid> centroids = new ArrayList<Centroid>();
  private ArrayList<String> locationKey = new ArrayList<String>();

  public FurthestPointKMeans(Integer clusterSize, double errorRate) {
    nbCluster = clusterSize;
    error = errorRate;
    radius = 0.0;
    errorRadius = 0.0;
  }

  public Integer classify(double[] features) {
    if(!isReady())
      throw new IllegalStateException("FurthestPointKMeans is not ready yet");
    else {
      return nearestCentroid(features); // Find nearest centroid
    }
  }

  public boolean update(double[] feature, String location) {
    boolean newCentroid = false;
    this.features.add(feature);
    this.locationKey.add(location);

    if(!isReady())
      newCentroid = initIfPossible();
    else {

      Integer centroidIdx = classify(feature);
      if(centroidIdx != null) {
        double[] centroid = centroids.get(centroidIdx).feature;

        if (MathUtil.euclideanDistance(centroid, feature) > errorRadius) {
          System.out.println("new distance " + MathUtil.euclideanDistance(centroid, feature));
          reset();
          newCentroid = initIfPossible();
        }
      }
    }

    return newCentroid;
  }

  public int clearFeatureFromLocation(String location) {

    int numRemoved = 0;
    Iterator featureIterator = features.iterator();
    Iterator<String> locationIterator = locationKey.iterator();

    while(locationIterator.hasNext() && featureIterator.hasNext()) {
      String value = locationIterator.next();
      featureIterator.next();

      if(value.matches(location)) {
        locationIterator.remove();
        featureIterator.remove();
        numRemoved++;
      }
    }

    return numRemoved;
  }

  public double[][] getCentroids() {
    double[][] centroid2DArray = new double[centroids.size()][];

    for(int i = 0; i < centroids.size(); i++)
      centroid2DArray[i] = centroids.get(i).feature;

    return centroid2DArray;
  }

  public boolean isLocationExist(String location) {
    return locationKey.contains(location);
  }

  public void reset() {
    centroids = new ArrayList<Centroid>();
    radius = 0.0;
    errorRadius = 0.0;
  }

  protected boolean isReady() {
    boolean centroidReady = (!this.centroids.isEmpty() && this.centroids.size() >= nbCluster);

    return centroidReady;
  }

  protected boolean initIfPossible()
  {
    boolean sufficientFeatureSize = features.size() >= 2 * nbCluster;

    if(sufficientFeatureSize) {
      initCentroids();
      return true;
    }
    else
      return false;
  }

  protected void initCentroids() {

    List<double[]> computeFeatures = copyFromFeatures();

    Random random = new Random();

    // Choose one centroid uniformly at random from among the data points.
    final double[] firstCentroid = computeFeatures.remove(random.nextInt(this.features.size()));
    this.centroids.add(new Centroid(firstCentroid));

    for (int j = 1; j < this.nbCluster; j++) {
      Integer idx = j + 1;
      int featureIdx = furthestFeature(computeFeatures);
      this.centroids.add(new Centroid(computeFeatures.remove(featureIdx)));
    }

    calculateRadius();
  }

  protected int furthestFeature(List<double[]> ncFeatures) {
    int maxIdx = Integer.MAX_VALUE;
    double MaxDistance = 0;
    for(int i = 0; i < ncFeatures.size() ; i++) {
      double distance = 0.0;

      for(Centroid c : centroids)
        distance += MathUtil.euclideanDistance(ncFeatures.get(i),c.feature);

      if(distance > MaxDistance) {
        MaxDistance = distance;
        maxIdx = i;
      }
    }
    return maxIdx;
  }

  protected Integer nearestCentroid(double[] feature) {
    // Find nearest centroid
    Integer nearestCentroidKey = null;

    Double minDistance = Double.MAX_VALUE;
    Double currentDistance;
    for (int idx = 0; idx < centroids.size(); idx++) {
      currentDistance = MathUtil.euclideanDistance(centroids.get(idx).feature, feature);
      if (currentDistance < minDistance) {
        minDistance = currentDistance;
        nearestCentroidKey = idx;
      }
    }

    return nearestCentroidKey;
  }

  private void calculateRadius() {

    double minDistance = Double.MAX_VALUE;

    for(int idx = 0; idx < centroids.size(); idx++) {
      for(int idx2 = idx+1; idx2 < centroids.size(); idx2++) {
        double dist = MathUtil.euclideanDistance(centroids.get(idx).feature,centroids.get(idx2).feature);
        if(dist < minDistance)
          minDistance = dist;
      }
    }

    radius = minDistance / 2;
    errorRadius = radius + (radius * (error / 2));

    System.out.println("ErrorRadius: " + errorRadius);
  }

  private List<double[]> copyFromFeatures() {
    List<double[]> copyFeatures = new ArrayList<double[]>();

    for(double[] data : features) {
      copyFeatures.add(data.clone());
    }

    return copyFeatures;
  }
}
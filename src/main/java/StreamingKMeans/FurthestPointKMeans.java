package StreamingKMeans;

import StreamingKMeans.util.Centroid;
import StreamingKMeans.util.Feature;
import StreamingKMeans.util.MathUtil;

import java.io.Serializable;
import java.util.*;

/**
 * Created by ken on 11/16/2015.
 */
@SuppressWarnings("Duplicates")
public class FurthestPointKMeans implements Clusterer, Serializable {

  private Integer nbCluster;
  private double radius;
  private double error;
  private double errorRadius;
  private ArrayList<Feature> features = new ArrayList<Feature>();
  private ArrayList<Centroid> centroids = new ArrayList<Centroid>();
  private long expireTimeMilis;
  private boolean removeOld;

  public FurthestPointKMeans(Integer clusterSize, double errorRate, long expireTime) {
    nbCluster = clusterSize;
    error = errorRate;
    expireTimeMilis = expireTime;
    if (expireTime > 0)
      removeOld = true;
    else
      removeOld = false;

    radius = 0.0;
    errorRadius = 0.0;
  }

  public Integer classify(Feature feature) {
    if(!isReady())
      throw new IllegalStateException("FurthestPointKMeans is not ready yet");
    else {
      return nearestCentroid(feature); // Find nearest centroid
    }
  }

  public boolean update(double[] feature, String location) {
    Feature newFeature = new Feature(feature,location);
    return update(newFeature);
  }

  public boolean update(Collection<Feature> features) {
    boolean updatedOnSomePoint = false;
    for(Feature f : features) {
      if (update(f))
        updatedOnSomePoint = true;
    }

    return updatedOnSomePoint;
  }

  public boolean update(Feature newF) {
    boolean isCentroidUpdated = false;

    if(removeOld)
      clearExpiredFeature();

    features.add(newF);

    if(!isReady())
      isCentroidUpdated = initIfPossible();
    else {
      Integer centroidIdx = classify(newF);

      if(centroidIdx >= 0) {
        Centroid centroid = centroids.get(centroidIdx);

        if (MathUtil.euclideanDistance(centroid.feature.value, newF.value) > errorRadius) {
          System.out.println("new distance " + MathUtil.euclideanDistance(centroid.feature.value, newF.value));
          reset();
          isCentroidUpdated = initIfPossible();
        }
      }
    }

    return isCentroidUpdated;
  }

  public int clearFeatureFromLocation(String location) {

    int numRemoved = 0;
    Iterator<Feature> featureIterator = features.iterator();

    while(featureIterator.hasNext()) {
      Feature f = featureIterator.next();

      if(f.location.matches(location)) {
        featureIterator.remove();
        numRemoved++;
      }
    }

    return numRemoved;
  }

  public List<Feature> getCentroids() {
    List<Feature> centroidFeatures = new ArrayList<Feature>();

    for(Centroid c : centroids)
      centroidFeatures.add(c.feature);

    return centroidFeatures;
  }

  public List<Feature> getFeatures() {
    return features;
  }

//  public boolean isLocationExist(String location) {
//    return locationKey.contains(location);
//  }

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
    boolean sufficientFeatureSize = features.size() >= 2 * nbCluster; //number of features where the KMeans start to compute the centroids

    if(sufficientFeatureSize) {
      initCentroids();
      return true;
    }
    else
      return false;
  }

  private void clearExpiredFeature() {
    long now = System.currentTimeMillis();

    Iterator<Feature> featureIterator = features.iterator();

    int i = 0;
    while(featureIterator.hasNext())
    {
      Feature f = featureIterator.next();
      if(now - f.createdTime > expireTimeMilis) {
        featureIterator.remove();
        i++;
      }
    }
//    if(i > 0)
//      System.out.println("Removed features: " + i);
  }

  private void initCentroids() {

    Random random = new Random();
    //List<Feature> computeFeatures = copyFromFeatures();
    List<Feature> computeFeatures = new ArrayList<Feature>(features); //shallow copy of current features

    // Choose one centroid uniformly at random from among the data points.
    Feature randomFeature = computeFeatures.remove(random.nextInt(computeFeatures.size()));
    centroids.add(new Centroid(randomFeature));

    //Get other centroids based on the feature with the furthest location from the chosen centroids
    for (int j = 1; j < this.nbCluster; j++) {
      int featureIdx = furthestFeature(computeFeatures);
      this.centroids.add(new Centroid(computeFeatures.remove(featureIdx)));
    }

    recalculateRadius();
  }

  private int furthestFeature(List<Feature> ncFeatures) {
    int maxIdx = Integer.MAX_VALUE;
    double MaxDistance = 0;
    for(Feature f : ncFeatures) {

      double distance = 0.0;

      for(Centroid c : centroids)
        distance += MathUtil.euclideanDistance(f.value,c.feature.value);

      if(distance > MaxDistance) {
        MaxDistance = distance;
        maxIdx = ncFeatures.indexOf(f);
      }
    }
    return maxIdx;
  }

  private int nearestCentroid(Feature f) {
    // Find nearest centroid
    int nearestCentroidIdx = -1;

    Double minDistance = Double.MAX_VALUE;
    Double distance;

    for (Centroid c : centroids) {
      distance = MathUtil.euclideanDistance(f.value, c.feature.value);

      if (distance < minDistance) {
        minDistance = distance;
        nearestCentroidIdx = centroids.indexOf(c);
      }
    }

    return nearestCentroidIdx;
  }

  private void recalculateRadius() {

    double minDistance = Double.MAX_VALUE;

    //find the closest distance between 2 centroids
    for(int idx = 0; idx < centroids.size(); idx++) {
      for(int idx2 = idx+1; idx2 < centroids.size(); idx2++) {
        double dist = MathUtil.euclideanDistance(
                centroids.get(idx).feature.value,
                centroids.get(idx2).feature.value);
        if(dist < minDistance)
          minDistance = dist;
      }
    }

    radius = minDistance / 2;
    errorRadius = radius + (radius * (error / 2));

    System.out.println("ErrorRadius: " + errorRadius);
  }

//  private List<Feature> copyFromFeatures() {
//    List<Feature> copyFeatures = new ArrayList<Feature>();
//
//    for(Feature f : features) {
//      copyFeatures.add(f);
//    }
//
//    return copyFeatures;
//  }
}
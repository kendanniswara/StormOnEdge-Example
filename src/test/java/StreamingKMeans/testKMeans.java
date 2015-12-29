package StreamingKMeans;

import StreamingKMeans.util.Feature;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import junit.framework.Assert;
import org.junit.Test;

/**
 * Created by ken on 11/17/2015.
 */
@SuppressWarnings("Duplicates")
public class testKMeans {

  @org.junit.Test
  public void RunKMeans() {

    FurthestPointKMeans localK1 = new FurthestPointKMeans(3,2, 8000);
    FurthestPointKMeans localK2 = new FurthestPointKMeans(3,2, 8000);
    FurthestPointKMeans globalK = new FurthestPointKMeans(3,2, -1);

    long i = 0;
    while(i < 50)
    {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      Feature f1;
      Feature f2;

      double[] values;

      values = new double[2];
      values[0] = ThreadLocalRandom.current().nextInt(5,45);
      values[1] = ThreadLocalRandom.current().nextInt(5,45);

      f1 = new Feature(values,"f1");

      values = new double[2];
      values[0] = ThreadLocalRandom.current().nextInt(55,95);
      values[1] = ThreadLocalRandom.current().nextInt(55,95);

      f2 = new Feature(values,"f2");

//      System.out.println("F1 : " + f1.value[0] + "," + f1.value[1]);
//      System.out.println("F2 : " + f2.value[0] + "," + f2.value[1]);

      boolean isK1update = localK1.update(f1);
      boolean isK2update = localK2.update(f2);

      if(isK1update)
      {
        System.out.println("[K1] new centroids:");
        printCentroids(localK1);
        globalK.clearFeatureFromLocation("f1");
        if(globalK.update(localK1.getCentroids())) {
          System.out.println("[globalK] centroids updated from K1");
          System.out.println("[globalK] new Centroids:");
          printCentroids(globalK);
        }
      }

      if(isK2update)
      {
        System.out.println("[K2] new centroids:");
        printCentroids(localK2);
        globalK.clearFeatureFromLocation("f2");
        if(globalK.update(localK2.getCentroids())) {
          System.out.println("[globalK] centroids updated from K2");
          System.out.println("[globalK] new Centroids:");
          printCentroids(globalK);
        }
      }



      i++;
    }

    //print everything
    System.out.println("[K1] Features:");
    List<Feature> features = localK1.getFeatures();
    for(Feature c : features) {
      System.out.println(c.value[0] + "," + c.value[1]);
    }
    System.out.println("[K2] Features:");
    features = localK2.getFeatures();
    for(Feature c : features) {
      System.out.println(c.value[0] + "," + c.value[1]);
    }
    System.out.println("[K1] Centroids:");
    printCentroids(localK1);
    System.out.println("[K2] Centroids:");
    printCentroids(localK2);
    System.out.println("[globalK] Centroids:");
    printCentroids(globalK);

  }

  public static void printCentroids(FurthestPointKMeans kmeans)
  {
    List<Feature> centroids = kmeans.getCentroids();
    for(Feature c : centroids) {
      System.out.println(c.value[0] + "," + c.value[1]);
    }
    System.out.println("---");
  }

}


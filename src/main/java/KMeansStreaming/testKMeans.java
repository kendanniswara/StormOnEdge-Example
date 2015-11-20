package KMeansStreaming;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by ken on 11/17/2015.
 */
public class testKMeans {



  public static void main(String[] argses) {
    long i = 0;

    FurthestPointKMeans k = new FurthestPointKMeans(4,2);
    Random r = new Random();
    int updated = 0;

    while(i < 9000000)
    {
      double[] feature = new double[2];

      if(i % 4 == 0) {
        feature[0] = ThreadLocalRandom.current().nextInt(10,40);
        feature[1] = ThreadLocalRandom.current().nextInt(10,40);
      } else if(i % 4 == 1) {
        feature[0] = ThreadLocalRandom.current().nextInt(10,40);
        feature[1] = ThreadLocalRandom.current().nextInt(60,90);
      } else if(i % 4 == 2) {
        feature[0] = ThreadLocalRandom.current().nextInt(60,90);
        feature[1] = ThreadLocalRandom.current().nextInt(10,40);
      } else {
        feature[0] = ThreadLocalRandom.current().nextInt(60,90);
        feature[1] = ThreadLocalRandom.current().nextInt(60,90);
      }

//      System.out.println(feature[0] + "," + feature[1]);

      Boolean update = k.update(feature, "");

      if(update)
      {
        updated++;
        double[][] centroids = k.getCentroids();
        System.out.println("[" + updated + "] new centroids:");
        for(int jj = 0; jj < centroids.length;jj++) {
          System.out.println(centroids[jj][0] + "," + centroids[jj][1]);
        }
        System.out.println("---");
      }

//      try {
//        Thread.sleep(2000);
//      } catch (InterruptedException e) {
//        e.printStackTrace();
//      }

      i++;
    }
  }

}


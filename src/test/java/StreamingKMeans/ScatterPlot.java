package StreamingKMeans;

/**
 * Created by ken on 12/29/2015.
 */
import javax.swing.*;
import java.awt.geom.*;
import java.awt.Graphics;
import java.util.*;

public class Scatterplot extends JFrame {

  private List points = new ArrayList();


  public Scatterplot() {
    super("Scatterplot");
    setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

    //adding points
//    for(int a = 0; a < c.myList.size(); a++)
//    {
//      points.add(new Point2D.Float(a, c.myList.get(a).getSteps()));
//    }
    //end adding points


    JPanel panel = new JPanel() {
      public void paintComponent(Graphics g) {
        for(Iterator i=points.iterator(); i.hasNext(); ) {
          Point2D.Float pt = (Point2D.Float)i.next();
          g.drawString("*", (int)(pt.x)+40,
                  (int)(-pt.y+getHeight())- 40);
        }
        int width = getWidth();
        int height = getHeight();
        setVisible(true);
        //axises (axes?)
        g.drawLine(0, height - 40, width, height-40);
        g.drawLine(40, height - 270, 40, height);

        //y-axis labels below
        for (int a = 1; a < 5; a++)
        {
          String temp = 20*a + "--";
          g.drawString(temp, 20, height - (36 + 20*(a)));
        }
        for (int a = 5; a < 11; a++)
        {
          String temp = 20*a + "--";
          g.drawString(temp, 11, height - (36 + 20*(a)));
        }
        //y-axis labels above

        //x-axis labels below
        for (int a = 1; a < 21; a++)
        {
          g.drawString("|", 40 + 50*a, height - 30);
          int x = 50*a;
          String temp = x + " ";
          g.drawString(temp, 30 + 50*a, height - 18);
        }
        g.drawString("The Collatz Conjecture: Number vs. Stopping Time", 400, 60);
        //x-axis labels above

      }
    };

    setContentPane(panel);
    //last two numbers below change the initial size of the graph.
    setBounds(20, 20, 1100, 400);
    setVisible(true);
  }
}

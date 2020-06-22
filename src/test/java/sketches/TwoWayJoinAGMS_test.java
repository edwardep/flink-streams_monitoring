package sketches;

import org.junit.Test;

import java.util.Arrays;

import static java.lang.Math.abs;
import static java.lang.Math.sqrt;
import static junit.framework.TestCase.assertTrue;
import static test_utils.Generators.uniform_random_vector;
import static sketches.SketchMath.*;

public class TwoWayJoinAGMS_test {


    @Test
    public void test_twoWay_join_agms_safezone1()
    {
        int d = 3;
        int w = 4;
        int D = d*w;

        double[] E, E1, E2;
        double E1E2, TLow, THigh;


        E = uniform_random_vector(2*D, 20, -10);

        E1 = Arrays.copyOfRange(E, 0, D);
        E2 = Arrays.copyOfRange(E, D,2*D);

        E1E2 = median(transform(E1,d,w), transform(E2,d,w));
        TLow = E1E2 - 0.1*abs(E1E2);
        THigh = E1E2 + 0.1*abs(E1E2);
        TwoWayJoinAGMS zeta = new TwoWayJoinAGMS(E, d, w, TLow, THigh, true);

        double zeta_E = zeta.inf(E);
        assertTrue(0.0 <= zeta_E);
        assertTrue(zeta.inf(scale(E,2.))<= 0.0);

        // Check safe zone conformity
        int count_safe = 0, count_admissible=0;
        int N = 1000;
        double rho = abs(zeta_E)/sqrt(2.0*D);
        for(int i = 0; i < N; i++) {
            double[] X = add(E, uniform_random_vector(2*D, 20.0*rho, -10.0*rho));

            double[] X1 = Arrays.copyOfRange(X, 0, D);
            double[] X2 = Arrays.copyOfRange(X, D,2*D);

            double X1X2 = median(transform(X1,d,w), transform(X2,d,w));

            int admissible = (TLow <= X1X2) && (X1X2 <= THigh) ? 1 : 0;
            if(admissible == 1) count_admissible++;

            int safe = (zeta.inf(X) >= 0) ? 1 : 0;
            if(safe == 1) count_safe++;

            assert safe <= admissible;
        }

        System.out.println("TwoWay Join safezone1 test:");
        System.out.println("admissible: " + count_admissible);
        System.out.println("safe: " + count_safe);
    }
}

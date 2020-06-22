package sketches;

import org.junit.Test;
import sketches.SelfJoinAGMS.SelfJoin_lowerBound;
import sketches.SelfJoinAGMS.SelfJoin_upperBound;

import static java.lang.Math.abs;
import static junit.framework.TestCase.assertTrue;
import static test_utils.Generators.uniform_random_vector;
import static sketches.SketchMath.add;
import static sketches.SketchMath.median;

public class SelfJoinAGMS_test {

    /**
     * Test the self-join upper bound for accuracy.
     * This is done by taking random sketches and comparing
     * to the distance of the admissible region.
     */
    @Test
    public void test_sj_upperBound() {

        for(int i = 0; i < 10; i++) {
            AGMSSketch E = new AGMSSketch(5, 10);

            // fill the sketch with random values
            for (int w = 0; w < E.width(); w++)
                E.setSketchColumn(w, uniform_random_vector(E.depth(), 20, -10));

            double Emed = median(E.values());

            SelfJoin_upperBound sz = new SelfJoin_upperBound(E.values(), 1.1 * Emed, true);

            assertTrue(0.0 < sz.median(E.values()));

            // test 100 sketches from each type
            int count_inA = 0, count_notInA = 0, count_inZ = 0, count_notInZ = 0;

            while (count_notInA < 100 || count_inA < 100) {
                AGMSSketch X = new AGMSSketch(5, 10);

                // X = E + rand
                for (int w = 0; w < X.width(); w++)
                    X.setSketchColumn(w, add(E.getSketchColumn(w), uniform_random_vector(X.depth(), 10, -5)));

                int inA = (median(X.values()) < 1.1 * Emed) ? 1 : 0;
                if (inA == 1) count_inA++;
                else count_notInA++;

                int inZ = (sz.median(X.values()) > 0) ? 1 : 0;
                if (inZ == 1) count_inZ++;
                else count_notInZ++;

                assert inZ <= inA;
            }
            System.out.println("Upper bound test:");
            System.out.println("in A: " + count_inA + ", not in A: " + count_notInA);
            System.out.println("in Z: " + count_inZ + ", not in Z: " + count_notInZ);
        }
    }

    /**
     * Test the self-join lower bound for accuracy.
     * This is done by taking random sketches and comparing
     * to the distance of the admissible region.
     */
    @Test
    public void test_sj_lowerBound() {
        AGMSSketch E = new AGMSSketch(5, 10);

        for(int i = 0; i < 10; i++) {

            // fill the sketch with random values
            for (int w = 0; w < E.width(); w++)
                E.setSketchColumn(w, uniform_random_vector(E.depth(), 20, -10));

            double Emed = median(E.values());

            SelfJoin_lowerBound sz;
            sz = new SelfJoin_lowerBound(E.values(), 0.9 * Emed, true);


            assertTrue(0.0 < sz.median(E.values()));

            // test 100 sketches from each type
            int count_inA = 0, count_notInA = 0, count_inZ = 0, count_notInZ = 0;

            while (count_notInA < 100 || count_inA < 100) {
                AGMSSketch X = new AGMSSketch(5, 10);

                // X = E + rand
                for (int w = 0; w < X.width(); w++)
                    X.setSketchColumn(w, add(E.getSketchColumn(w), uniform_random_vector(X.depth(), 10, -5)));

                int inA = (median(X.values()) >= 0.9 * Emed) ? 1 : 0;
                if (inA == 1) count_inA++;
                else count_notInA++;

                int inZ = (sz.median(X.values()) > 0) ? 1 : 0;
                if (inZ == 1) count_inZ++;
                else count_notInZ++;

                assert inZ <= inA;
            }
            System.out.println("Lower bound test:");
            System.out.println("in A: " + count_inA + ", not in A: " + count_notInA);
            System.out.println("in Z: " + count_inZ + ", not in Z: " + count_notInZ);
        }
    }

    /**
     * Testing the self-join complete safezone.
     * Not implemented COMPARE_WITH_BOUNDING_BALLS segment
     */
    @Test
    public void test_selfjoin() {
        AGMSSketch E = new AGMSSketch(5, 10);

        for(int i = 0; i < 10; i++) {

            // fill the sketch with random values
            for (int w = 0; w < E.width(); w++)
                E.setSketchColumn(w, uniform_random_vector(E.depth(), 20, -10));

            double Emed = median(E.values());

            SelfJoinAGMS sz = new SelfJoinAGMS(E.values(), 0.9*Emed, 1.1*Emed, true);


            assertTrue(0.0 < sz.zeta(E.values()));

            // test 100 sketches from each type
            int count_inA = 0, count_notInA = 0, count_inZ = 0, count_notInZ = 0;

            for(int j = 0; j < 1000; j++) {
                AGMSSketch X = new AGMSSketch(5, 10);

                // X = E + rand
                for (int w = 0; w < X.width(); w++)
                    X.setSketchColumn(w, add(E.getSketchColumn(w), uniform_random_vector(X.depth(), 4,-2)));//2*1.65, -1.65)));

                int inA = (abs(median(X.values()) - Emed) <= 0.1*Emed) ? 1 : 0;
                if(inA == 1) count_inA++;
                else count_notInA++;

                int inZ = (sz.zeta(X.values()) > 0) ? 1 : 0;
                if (inZ == 1) count_inZ++;
                else count_notInZ++;

                assert inZ <= inA;
            }
            System.out.println("Both bounds test:");
            System.out.println("in A: " + count_inA + ", not in A: " + count_notInA);
            System.out.println("in Z: " + count_inZ + ", not in Z: " + count_notInZ);
        }
    }
}

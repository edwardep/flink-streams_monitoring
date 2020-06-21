package sketches;

import org.junit.Test;

import java.util.Arrays;

import static junit.framework.TestCase.*;
import static test_utils.Generators.const_vector;
import static test_utils.Generators.const_vector2;
import static sketches.SketchMath.*;

public class SketchMath_test {

    @Test
    public void sketch_init_test() {
        AGMSSketch sketch = new AGMSSketch(3,5);
        AGMSSketch zeros = const_vector(3,5,0);

        assertEquals(sketch, zeros);
    }

    @Test
    public void add_sketches_test() {
        AGMSSketch vec10 = const_vector(3,5,10);

        // identity property
        AGMSSketch vec0 = const_vector(3,5,0);
        assertEquals(vec10, add(vec10, vec0));

        // commutative property
        AGMSSketch vec2 = const_vector(3,5,2);
        assertEquals(add(vec10, vec2), add(vec2, vec10));

        // inverse property
        AGMSSketch vec_10 = const_vector(3,5,-10);
        assertEquals(vec0, add(vec10, vec_10));

        // correctness
        AGMSSketch vec12 = const_vector(3,5,12);
        assertEquals(vec12, add(vec10, vec2));
    }

    @Test
    public void add_arrays_test() {
        Double[] vec10 = const_vector(5,10);

        // identity property
        Double[] vec0 = const_vector(5,0);
        assert Arrays.equals(vec10, add(vec0, vec10));

        // commutative property
        Double[] vec2 = const_vector(5,2);
        assert Arrays.equals(add(vec10, vec2), add(vec2, vec10));

        // inverse property
        Double[] vec_10 = const_vector(5,-10);
        assert Arrays.equals(vec0, add(vec10, vec_10));

        // correctness
        Double[] vec12 = const_vector(5,12);
        assert Arrays.equals(vec12, add(vec10, vec2));
    }

    @Test
    public void subtract_sketches_test() {
        AGMSSketch vec10 = const_vector(3,5,10);

        // identity property
        AGMSSketch vec0 = const_vector(3,5,0);
        assertEquals(vec10, subtract(vec10, vec0));

        // commutative property
        AGMSSketch vec2 = const_vector(3,5,2);
        assertNotSame(subtract(vec10, vec2), subtract(vec2, vec10));

        // correctness
        AGMSSketch vec8 = const_vector(3,5,8);
        assertEquals(vec8, subtract(vec10, vec2));
    }

    @Test
    public void subtract_arrays_test() {
        Double[] vec10 = const_vector(5,10);

        // identity property
        Double[] vec0 = const_vector(5,0);
        assert Arrays.equals(vec10, subtract(vec10, vec0));

        // commutative property
        Double[] vec2 = const_vector(5,2);
        assert !Arrays.equals(subtract(vec10, vec2), subtract(vec2, vec10));

        // correctness
        Double[] vec8 = const_vector(5,8);
        assert Arrays.equals(vec8, subtract(vec10, vec2));
    }

    @Test
    public void multiply_arrays_test() {
        Double[] vec10 = const_vector(5,10);

        // identity property
        Double[] vec1 = const_vector(5,1);
        assert Arrays.equals(vec10, multiply(vec10, vec1));

        // commutative property
        Double[] vec2 = const_vector(5,2);
        assert Arrays.equals(multiply(vec10, vec2), multiply(vec2, vec10));

        // inverse property
        Double[] vec1_10 = const_vector(5,1.0/10);
        assert Arrays.equals(vec1, multiply(vec10, vec1_10));

        // correctness
        Double[] vec20 = const_vector(5,20);
        assert Arrays.equals(vec20, multiply(vec10, vec2));
    }
    @Test
    public void sqrt_array_test() {
        Double[] vec25 = const_vector(5,25);

        // correctness
        Double[] vec5 = const_vector(5,5);
        assert Arrays.equals(vec5, sqrt(vec25));
    }

    @Test
    public void scale_sketch_test() {
        AGMSSketch vec10 = const_vector(3,5,10);

        // identity property
        assertEquals(vec10, scale(vec10, 1));

        // correctness
        AGMSSketch vec20 = const_vector(3,5,20);
        assertEquals(vec20, scale(vec10, 2));

        // inverse
        AGMSSketch vec5 = const_vector(3,5,5);
        assertEquals(vec5, scale(vec10, 1.0/2));
    }

    @Test
    public void scale_array_test() {
        Double[] vec10 = const_vector(5,10);

        // identity property
        assert Arrays.equals(vec10, scale(vec10, 1));

        // correctness
        Double[] vec20 = const_vector(5,20);
        assert Arrays.equals(vec20, scale(vec10, 2));

        // inverse
        Double[] vec5 = const_vector(5,5);
        assert Arrays.equals(vec5, scale(vec10, 1.0/2));
    }

    @Test
    public void norm_of_array_test() {
        Double[] vec = const_vector(3, 5);

        // norm of 1D array
        double expected_norm = Math.sqrt(5*5 + 5*5 + 5*5);
        assertEquals(expected_norm, norm(vec));

        // norm per row of 2D array
        Double[][] vec2 = const_vector2(3,3,5);
        Double[] normOfVec2 = const_vector(3, expected_norm);
        assert Arrays.deepEquals(normOfVec2, normRow(vec2));
    }

    @Test
    public void normalize_array_test() {
        double expected_norm = Math.sqrt(5*5 + 5*5 + 5*5);
        Double[] norm = const_vector(3, expected_norm);

        // normalize 1d array
        Double[] vec = const_vector(3*5, 5);
        Double[] normalizedVector = normalize(vec, norm);

        for(int i = 0; i < vec.length; i++)
            assertEquals(vec[i]/expected_norm, normalizedVector[i]);

        // normalize 2d array
        Double[][] vec2 = const_vector2(3, 5, 5);
        Double[][] normalizedVector2 = normalize(vec2, norm);

        for(int i = 0; i < vec2.length; i++) {
            for(int j = 0; j < vec2[0].length; j++) {
                assertEquals(vec2[i][j]/expected_norm, normalizedVector2[i][j]);
            }
        }

        // compare the 2 methods by projecting the 1d array to 2d space (depends on SketchMath.transform())
        assert Arrays.deepEquals(normalizedVector2, transform(normalizedVector, 3, 5));
    }

    @Test
    public void dotProduct_test() {
        Double[][] vec1 = const_vector2(3, 5, 5);
        Double[][] vec2 = const_vector2(3,5,2);

        Double[] expectedPerRow = const_vector(3, 5*2 + 5*2 + 5*2 + 5*2 + 5*2);

        assert Arrays.equals(expectedPerRow, dotProduct(vec1, vec2));
    }

    @Test
    public void median_test() {
        Double[] odd = new Double[]{5.0, 2.0, 13.0, 4.0, -2.0};
        assertEquals(4.0, median(odd));

        // on even size it will pick the n/2 + 1 element
        Double[] even = new Double[]{10.0, 2.0, -5.0, 4.0};
        assertEquals(4.0, median(even));
    }
    @Test
    public void transform_1d_2d_test() {
        Double[][] vec2d = const_vector2(3,5, 5);
        Double[] vec = const_vector(3*5, 5);

        assert Arrays.deepEquals(vec2d, transform(vec, 3, 5));
    }
}

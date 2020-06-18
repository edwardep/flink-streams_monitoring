package utils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SketchOperators {

    public static Double[] sum(Double[] col1, Double[] col2) {
        assert col1.length == col2.length;
        Double[] ret = new Double[col1.length];
        Arrays.fill(ret,0d);
        for(int d = 0; d < col1.length; d++)
            ret[d] = col1[d] + col2[d];
        return ret;
    }

    /**
     * Used in aggregateSketch method.
     * @param col1  The previous value
     * @param col2  The input value to be aggregated
     * @param k Scalar for the input value e.g for averaging k = number of inputs
     * @return Sum of the 2 columns
     */
    public static Double[] sum(Double[] col1, Double[] col2, int k) {
        assert col1.length == col2.length;
        Double[] ret = new Double[col1.length];
        Arrays.fill(ret,0d);
        for(int d = 0; d < col1.length; d++)
            ret[d] = col1[d] + col2[d] / k;
        return ret;
    }

    public static Double[] subtract(Double[] col1, Double[] col2) {
        assert col1.length == col2.length;
        Double[] ret = new Double[col1.length];
        Arrays.fill(ret,0d);
        for(int d = 0; d < col1.length; d++)
            ret[d] = col1[d] - col2[d];
        return ret;
    }

    public static Double[] multiply(Double[] col1, Double[] col2) {
        assert col1.length == col2.length;
        Double[] ret = new Double[col1.length];
        Arrays.fill(ret,0d);
        for(int d = 0; d < col1.length; d++)
            ret[d] = col1[d] * col2[d];
        return ret;
    }

    public static Double[] sqrt(Double[] col) {
        Double[] ret = new Double[col.length];
        Arrays.fill(ret,0d);
        for(int d = 0; d < col.length; d++)
            ret[d] = Math.sqrt(col[d]);
        return ret;
    }


    public static Double[] divide(Double[] col1, Double[] col2) {
        assert col1.length == col2.length;
        Double[] ret = new Double[col1.length];
        Arrays.fill(ret,0d);
        for(int d = 0; d < col1.length; d++)
            if(col2[d] != 0) ret[d] = col1[d] / col2[d];
        return ret;
    }

    public static Double[] scale(Double[] col, Double scalar) {
        Double[] ret = new Double[col.length];
        Arrays.fill(ret,0d);
        for(int d = 0; d < col.length; d++)
            ret[d] = col[d] * scalar;
        return ret;
    }

    public static Double[] norm(Iterable<Map.Entry<Integer, Double[]>> vector) {
        Double[] res = new Double[vector.iterator().next().getValue().length];
        Arrays.fill(res,0d);
        for (Map.Entry<Integer, Double[]> entry : vector)
            res = sum(res, multiply(entry.getValue(), entry.getValue()));

        for(int d = 0; d < res.length; d++)
            res[d] = Math.sqrt(res[d]);
        return res;
    }

    public static Double norm(Double[] vector) {
        double res = 0d;
        for (Double e : vector) res += e*e;
        return Math.sqrt(res);
    }

    public static Double[] normRow(Double[][] vector) {
        Double[] res = new Double[vector.length];
        Arrays.fill(res,0d);
        for(int w = 0; w < vector[0].length; w++) {
            for(int d = 0; d < vector.length; d++) {
                res[d] += vector[d][w]*vector[d][w];
            }
        }
        for(int d = 0; d < res.length; d++)
            res[d] = Math.sqrt(res[d]);
        return res;
    }

    public static Double[] normRow(Double[] vector) {
        Double[] ret = new Double[vector.length];
        return ret;
    }


    public static Map<Integer, Double[]> normalize(Iterable<Map.Entry<Integer, Double[]>> vector, Double[] norm) {
        Map<Integer, Double[]> res = new HashMap<>();
        for (Map.Entry<Integer, Double[]> entry : vector)
            res.put(entry.getKey(), divide(entry.getValue(), norm));
        return res;
    }

    public static Double[][] normalize(Double[][] vector, Double[] norm) {
        Double[][] res = new Double[vector.length][vector[0].length];
        for(int w = 0; w < vector[0].length; w++){
            for(int d = 0; d < vector.length; d++){
                if(norm[d] > 0)
                    res[d][w] = vector[d][w] / norm[d];
            }
        }
        return res;
    }

    //not checked
    public static Double[] normalize(Double[] vector, Double[] norm) {
        assert vector.length % norm.length == 0;
        int w = vector.length / norm.length;
        Double[] res = new Double[vector.length];
        for(int i = 0; i < vector.length; i++)
            res[i] = vector[i] / norm[i / w];
        return res;
    }

    public static Double[] dotProduct(Map<Integer, Double[]> A, Map<Integer, Double[]> B) {
        Double[] product = new Double[A.get(0).length];
        Arrays.fill(product,0d);
        for (Integer w : A.keySet())
            product = sum(product, multiply(A.get(w), B.get(w)));
        return product;
    }

    public static Double[] dotProduct(Double[][] A, Double[][] B) {
        assert (A.length == B.length) && (A[0].length == B[0].length);
        Double[] product = new Double[A.length];
        Arrays.fill(product, 0d);
        for(int w = 0; w < A[0].length; w++){
            for(int d = 0; d < A.length; d++){
                product[d] += A[d][w] * B[d][w];
            }
        }
        return product;
    }

    public static Double median(Double[] queryVector) {
        Arrays.sort(queryVector);
        return queryVector[(queryVector.length)/2];
    }

    public static Double median(Double[][] sk1, Double[][] sk2) {
        Double[] queryVector = new Double[sk1.length];
        Arrays.fill(queryVector, 0d);
        for(int w = 0; w < sk1[0].length; w++) {
            for (int d = 0; d < sk1.length; d++) {
                queryVector[d] += sk1[d][w] * sk2[d][w];
            }
        }
        return median(queryVector);
    }

    public static Double median(Double[][] sk) {
        Double[] queryVector = new Double[sk.length];
        Arrays.fill(queryVector, 0d);
        for(int w = 0; w < sk[0].length; w++) {
            for (int d = 0; d < sk.length; d++) {
                queryVector[d] += sk[d][w] * sk[d][w];
            }
        }
        return median(queryVector);
    }

    /**
     * Transforming 1d vector to 2d vector for use in sketch operations
     * @param vec   the source vector
     * @param depth     the resulting vector's depth
     * @param width     the resulting vector's width
     * @return  2d vector
     */
    public static Double[][] transform(Double[] vec, int depth, int width) {
        Double[][] ret = new Double[depth][width];
        for(int i = 0; i < depth; i++) {
            System.arraycopy(vec, (i*width), ret[i], 0, width);
        }
        return ret;
    }
}


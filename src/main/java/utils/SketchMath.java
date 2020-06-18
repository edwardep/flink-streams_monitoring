package utils;

import sketches.AGMSSketch;

public class SketchMath {

    public static AGMSSketch add(AGMSSketch sk1, AGMSSketch sk2){
        assert sk1.depth() == sk2.depth() && sk1.width() == sk2.width();
        AGMSSketch res = new AGMSSketch(sk1.depth(), sk1.width());

        for(int i = 0; i < sk1.depth(); i++){
            for(int j = 0; j < sk1.width(); j++)
                res.values()[i][j] = sk1.values()[i][j] + sk2.values()[i][j];
        }
        return res;
    }

    public static AGMSSketch subtract(AGMSSketch sk1, AGMSSketch sk2){
        assert sk1.depth() == sk2.depth() && sk1.width() == sk2.width();
        AGMSSketch res = new AGMSSketch(sk1.depth(), sk1.width());

        for(int i = 0; i < sk1.depth(); i++){
            for(int j = 0; j < sk1.width(); j++)
                res.values()[i][j] = sk1.values()[i][j] - sk2.values()[i][j];
        }
        return res;
    }

    public static AGMSSketch scale(AGMSSketch sk1, double scalar){
        AGMSSketch res = new AGMSSketch(sk1.depth(), sk1.width());

        for(int i = 0; i < sk1.depth(); i++){
            for(int j = 0; j < sk1.width(); j++)
                res.values()[i][j] = sk1.values()[i][j]*scalar;
        }
        return res;
    }

}

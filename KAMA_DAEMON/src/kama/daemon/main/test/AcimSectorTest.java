package kama.daemon.main.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import kama.daemon.common.util.model.BoundXY;
import kama.daemon.common.util.model.GridCalcUtil;
import kama.daemon.common.util.model.ModelGridUtil;
import kama.daemon.common.util.model.PointLonLat;
import kama.daemon.common.util.model.PointXY;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class AcimSectorTest {

    public static void main(String[] args) throws Exception {

        String latPath = "F:\\data\\datastore\\grid\\kim_gktg_lat.bin";
        String lonPath = "F:\\data\\datastore\\grid\\kim_gktg_lon.bin";

        ModelGridUtil modelGridUtil =
            new ModelGridUtil(ModelGridUtil.Model.KIM_GKTG, latPath, lonPath);

        modelGridUtil.setMultipleGridBoundInfoforLatLonGrid(50, 20, 110, 150);

        BoundXY boundXY = modelGridUtil.getBoundXY();

        int cropLeft = boundXY.getLeft();
        int cropRight = boundXY.getRight();

        // 중요:
        // 좌표파일 / NetCDF / koreaData는 위도가 낮은 쪽부터 저장됨.
        // 따라서 Y index는 bottom이 작고, top이 큼.
        int cropBottom = boundXY.getBottom(); // 저위도, 작은 Y index
        int cropTop = boundXY.getTop();       // 고위도, 큰 Y index

        System.out.println(":: crop 원본 좌표계");
        System.out.println("left   = " + cropLeft);
        System.out.println("right  = " + cropRight);
        System.out.println("bottom = " + cropBottom + "  // 저위도, 작은 index");
        System.out.println("top    = " + cropTop + "  // 고위도, 큰 index");

        NetcdfDataset ncFile = NetcdfDataset.acquireDataset(
            "F:\\KAMA_AAMI\\2026\\항기청_수신\\항기청_수신_20260318\\ACIM_CNVT\\amo_kimg_acim_cnvt_f00_2026031800.nc",
            null
        );

        Variable var = ncFile.findVariable("CCT");

        List<Range> cropRange = new ArrayList<>();
        cropRange.add(new Range(cropBottom, cropTop));
        cropRange.add(new Range(cropLeft, cropRight));

        int rows = modelGridUtil.getRows();
        int cols = modelGridUtil.getCols();

        Float[][] koreaData =
            GridCalcUtil.convertStorageToValues(var.read(cropRange).getStorage(), rows, cols);

        System.out.println(":: koreaData 기준");
        System.out.println("koreaData[0][0] = 원본 좌표 (" + cropLeft + ", " + cropBottom + ")");
        System.out.println("koreaData[y][x] = 원본 좌표 (cropLeft + x, cropBottom + y)");
        System.out.println("first value = " + koreaData[0][0]);

        List<double[]> polygon = Arrays.asList(
        	    new double[]{124.0000, 38.0000},
        	    new double[]{124.8500, 38.0000},
        	    new double[]{127.6644, 38.3389},
        	    new double[]{127.6644, 37.0361},
        	    new double[]{127.5611, 36.9711},
        	    new double[]{127.2311, 37.1194},
        	    new double[]{126.9375, 37.0839},
        	    new double[]{125.8061, 36.9472},
        	    new double[]{125.6000, 36.9086},
        	    new double[]{125.6000, 36.3333},
        	    new double[]{124.0000, 36.3333},
        	    new double[]{124.0000, 38.0000}
        	);

        double[] extent = getPolygonExtent(polygon);
        double topLat = extent[0];
        double bottomLat = extent[1];
        double leftLon = extent[2];
        double rightLon = extent[3];

        PointXY polygonLeftTop = modelGridUtil.getPointXY(leftLon, topLat);
        PointXY polygonRightBottom = modelGridUtil.getPointXY(rightLon, bottomLat);

        int polygonLeft = polygonLeftTop.getX();
        int polygonRight = polygonRightBottom.getX();

        // 여기서도 Y를 뒤집지 않는다.
        // getPointXY가 반환하는 Y는 원본 좌표파일 index 기준이다.
        int polygonTop = polygonLeftTop.getY();
        int polygonBottom = polygonRightBottom.getY();

        System.out.println(":: polygon 원본 좌표계");
        System.out.println("left   = " + polygonLeft);
        System.out.println("right  = " + polygonRight);
        System.out.println("bottom = " + polygonBottom);
        System.out.println("top    = " + polygonTop);

        int startX = Math.max(polygonLeft, cropLeft);
        int endX = Math.min(polygonRight, cropRight);
        int startY = Math.max(polygonBottom, cropBottom);
        int endY = Math.min(polygonTop, cropTop);

        System.out.println(":: 실제 loop 범위");
        System.out.println("x = " + startX + " ~ " + endX);
        System.out.println("y = " + startY + " ~ " + endY);

        System.out.println(":: 좌표 변환 검증");
        printCompareValue(var, koreaData, cropLeft, cropBottom, polygonLeft, polygonTop);
        printCompareValue(var, koreaData, cropLeft, cropBottom, polygonRight, polygonBottom);

        System.out.println(":: 격자출력");
        System.out.println("==================================================================================");

        for (int modelY = startY; modelY <= endY; modelY++) {
            for (int modelX = startX; modelX <= endX; modelX++) {

                int dataX = modelX - cropLeft;
                int dataY = modelY - cropBottom;

                Float value = koreaData[dataY][dataX];

                PointLonLat lonLat = modelGridUtil.getPointLonLat(modelX, modelY);

                boolean inPolygon = isPointInPolygon(
                    polygon,
                    lonLat.getLon(),
                    lonLat.getLat()
                );

                if (inPolygon) {
//                    if (value != null && value >= 180) {
//                        System.out.print("* ");
//                    } else {
                        System.out.print("0 ");
//                    }
                } else {
                    System.out.print("- ");
                }
            }
            System.out.println();
        }

        ncFile.close();
    }

    private static void printCompareValue(Variable var,
                                          Float[][] koreaData,
                                          int cropLeft,
                                          int cropBottom,
                                          int modelX,
                                          int modelY) throws Exception {

        List<Range> rangeList = new ArrayList<>();
        rangeList.add(new Range(modelY, modelY));
        rangeList.add(new Range(modelX, modelX));

        float[] ncValue = (float[]) var.read(rangeList).getStorage();

        int dataX = modelX - cropLeft;
        int dataY = modelY - cropBottom;

        Float cropValue = koreaData[dataY][dataX];

        System.out.println("modelXY = (" + modelX + ", " + modelY + ")");
        System.out.println("dataXY  = (" + dataX + ", " + dataY + ")");
        System.out.println("nc      = " + ncValue[0]);
        System.out.println("crop    = " + cropValue);
        System.out.println();
    }

    public static boolean isPointInPolygon(List<double[]> polygon, double x, double y) {
        int n = polygon.size();
        boolean inside = false;

        for (int i = 0, j = n - 1; i < n; j = i++) {
            double xi = polygon.get(i)[0], yi = polygon.get(i)[1];
            double xj = polygon.get(j)[0], yj = polygon.get(j)[1];

            boolean intersect = ((yi > y) != (yj > y)) &&
                (x < (xj - xi) * (y - yi) / (yj - yi + 0.0) + xi);

            if (intersect) {
                inside = !inside;
            }
        }

        return inside;
    }

    public static double[] getPolygonExtent(List<double[]> polygon) {
        if (polygon == null || polygon.isEmpty()) {
            throw new IllegalArgumentException("polygon이 비어있습니다.");
        }

        double top = Double.NEGATIVE_INFINITY;
        double bottom = Double.POSITIVE_INFINITY;
        double left = Double.POSITIVE_INFINITY;
        double right = Double.NEGATIVE_INFINITY;

        for (double[] point : polygon) {
            double lon = point[0];
            double lat = point[1];

            if (lat > top) top = lat;
            if (lat < bottom) bottom = lat;
            if (lon < left) left = lon;
            if (lon > right) right = lon;
        }

        return new double[]{top, bottom, left, right};
    }
}
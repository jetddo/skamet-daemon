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

public class ModelGridTest3 {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
//		
		
		String latPath = "F:\\data\\datastore\\grid\\kim_gktg_lat.bin";
		String lonPath = "F:\\data\\datastore\\grid\\kim_gktg_lon.bin";
		
		ModelGridUtil modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KIM_GKTG, latPath, lonPath);
		
		modelGridUtil.setMultipleGridBoundInfoforLatLonGrid(50, 20, 110, 150);
		BoundXY boundXY = modelGridUtil.getBoundXY();
		
		int rows = modelGridUtil.getRows();
		int cols = modelGridUtil.getCols();
		
		System.out.println(":: 한반도영역 원본 좌표계");
		System.out.println(boundXY.getLeft() + ", " + boundXY.getRight() + ", " + boundXY.getTop() + ", " + boundXY.getBottom());		
		
		NetcdfDataset ncFile = NetcdfDataset.acquireDataset("F:\\KAMA_AAMI\\2026\\항기청_수신\\항기청_수신_20260318\\ACIM_CNVT\\amo_kimg_acim_cnvt_f00_2026031800.nc", null);
		
		System.out.println(ncFile.getLocation());
		
		Variable var = ncFile.findVariable("CCT");
		
		List<Range> rangeList = new ArrayList<Range>();
		rangeList.add(new Range(boundXY.getBottom(), boundXY.getTop()));
		rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
		
		Float[][] values = GridCalcUtil.convertStorageToValues(var.read(rangeList).getStorage(), rows, cols);
		
		System.out.println(":: 한반도영역 모델 좌표계");
		System.out.println(boundXY.getLeft() + ", " + boundXY.getRight() + ", " + (modelGridUtil.getModelHeight() - boundXY.getTop() - 1) + ", " + (modelGridUtil.getModelHeight() - boundXY.getBottom() - 1));		

		int cropModelLeft = boundXY.getLeft();
		int cropModelRight = boundXY.getRight();
		int cropModelTop = modelGridUtil.getModelHeight() - boundXY.getTop() - 1;
		int cropModelBottom = modelGridUtil.getModelHeight() - boundXY.getBottom() - 1;
		
		
		System.out.println(":: 첫번째 값");
		System.out.println(values[0][0]); // 0, 0 위치의 값);
		
		//50, 20, 110, 150
		
		
		
		// 별모양 폴리곤
		List<double[]> polygon = Arrays.asList(
			    new double[]{127.1911, 36.7386},
			    new double[]{127.3486, 36.8383},
			    new double[]{127.5611, 36.9711},
			    new double[]{129.0811, 35.9036},
			    new double[]{129.1569, 35.4117},
			    new double[]{130.1700, 35.4117},
			    new double[]{129.8847, 35.2089},
			    new double[]{129.3231, 34.7886},
			    new double[]{129.2333, 34.7231},
			    new double[]{129.1667, 34.6667},
			    new double[]{129.0178, 34.5031},
			    new double[]{128.4989, 34.5031},
			    new double[]{128.4989, 34.7653},
			    new double[]{128.4989, 35.2103},
			    new double[]{127.8311, 35.5031},
			    new double[]{127.8186, 35.5178},
			    new double[]{127.6144, 35.7531},
			    new double[]{127.6144, 35.8636},
			    new double[]{127.6144, 36.2031},
			    new double[]{127.1911, 36.7386}
			);
		
		double[] polygonExtent = getPolygonExtent(polygon);
		
		
		PointXY polygonLeftTop = modelGridUtil.getPointXY(polygonExtent[2], polygonExtent[0]);
		
//		System.out.println(":: 폴리곤 Left Top 원본 좌표");
//		System.out.println(polygonLeftTop.getX() + ", " + polygonLeftTop.getY());
		System.out.println(":: 폴리곤 Left Top 모델 좌표");
		
		int leftTopX = polygonLeftTop.getX();
		int leftTopY = modelGridUtil.getModelHeight() - polygonLeftTop.getY() - 1;
		
		System.out.println(leftTopX + ", " + leftTopY);
		
		System.out.println(":: 폴리곤 Left Top 값");
		rangeList = new ArrayList<Range>();
		rangeList.add(new Range(modelGridUtil.getModelHeight() - polygonLeftTop.getY() - 1, modelGridUtil.getModelHeight() - polygonLeftTop.getY() - 1));
		rangeList.add(new Range(polygonLeftTop.getX(), polygonLeftTop.getX()));
		float[] values1 = (float[])var.read(rangeList).getStorage();
		
		System.out.println(values1[0]); // 폴리곤 Left Top 위치의 값);
		
		System.out.println(values[cropModelBottom - leftTopY][leftTopX - cropModelLeft]); // 폴리곤 영역의 값]);
		
		PointXY polygonRightBottom = modelGridUtil.getPointXY(polygonExtent[3], polygonExtent[1]);
//		System.out.println(":: 폴리곤 Right Bottom 원본 좌표");
//		System.out.println(polygonRightBottom.getX() + ", " + polygonRightBottom.getY());
		System.out.println(":: 폴리곤 Right Bottom 모델 좌표");
		
		int rightBottomX = polygonRightBottom.getX();
		int rightBottomY = modelGridUtil.getModelHeight() - polygonRightBottom.getY() - 1;		
		
		System.out.println(rightBottomX + ", " + rightBottomY);
		
		System.out.println(":: 폴리곤 Right Bottom 값");
		rangeList = new ArrayList<Range>();
		rangeList.add(new Range(modelGridUtil.getModelHeight() - polygonRightBottom.getY() - 1, modelGridUtil.getModelHeight() - polygonRightBottom.getY() - 1));
		rangeList.add(new Range(polygonRightBottom.getX(), polygonRightBottom.getX()));
		values1 = (float[])var.read(rangeList).getStorage();
		
		System.out.println(values1[0]); // 폴리곤 Right Bottom 위치의 값);
		
		System.out.println(values[cropModelBottom - rightBottomY][rightBottomX - cropModelLeft]); // 폴리곤 영역의 값]);
		
		
		System.out.println(":: 격자출력");
		System.out.println("==================================================================================");
		
		for (int i = leftTopY; i <= rightBottomY; i++) {
			for (int j = leftTopX; j <= rightBottomX; j++) {				
				
				//System.out.println(values[cropModelBottom - i][j - cropModelLeft] + " "); // 폴리곤 영역의 값]);
				
				PointLonLat pointLonLat = modelGridUtil.getPointLonLat(j, modelGridUtil.getModelHeight() - 1 - i);
				
				boolean isInPolygon = isPointInPolygon(polygon, pointLonLat.getLon(), pointLonLat.getLat());
				
				//System.out.println("Point: " + pointLonLat.getLon() + ", " + pointLonLat.getLat() + " is in polygon: " + isInPolygon);
				
				if (isInPolygon) {
					
					if(values[cropModelBottom - i][j - cropModelLeft] >= 180) {
						System.out.print("* ");
					} else {
						System.out.print("0 ");
					}
					
					
				} else {
					System.out.print("- ");
				}
				
			}
			System.out.println();
		}		
		
		ncFile.close();
	}
	
	 /**
     * @param polygon 각 꼭짓점이 [경도, 위도]로 구성된 List<double[]>
     * @param x 검사할 점의 경도
     * @param y 검사할 점의 위도
     * @return 점이 폴리곤 내부에 있으면 true, 아니면 false
     */
    public static boolean isPointInPolygon(List<double[]> polygon, double x, double y) {
        int n = polygon.size();
        boolean inside = false;
        for (int i = 0, j = n - 1; i < n; j = i++) {
            double xi = polygon.get(i)[0], yi = polygon.get(i)[1];
            double xj = polygon.get(j)[0], yj = polygon.get(j)[1];

            boolean intersect = ((yi > y) != (yj > y)) &&
                    (x < (xj - xi) * (y - yi) / (yj - yi + 0.0) + xi);
            if (intersect) inside = !inside;
        }
        return inside;
    }
    
    /**
     * polygon의 extent(경계) 정보를 반환합니다.
     * @param polygon 각 꼭짓점이 [경도, 위도]로 구성된 List<double[]>
     * @return [top, bottom, left, right] 순서의 double 배열
     */
    public static double[] getPolygonExtent(List<double[]> polygon) {
        if (polygon == null || polygon.isEmpty()) {
            throw new IllegalArgumentException("polygon이 비어있습니다.");
        }
        double top = Double.NEGATIVE_INFINITY;
        double bottom = Double.POSITIVE_INFINITY;
        double left = Double.POSITIVE_INFINITY;
        double right = Double.NEGATIVE_INFINITY;

        for (double[] point : polygon) {
            double x = point[0]; // 경도
            double y = point[1]; // 위도

            if (y > top) top = y;
            if (y < bottom) bottom = y;
            if (x < left) left = x;
            if (x > right) right = x;
        }
        return new double[]{top, bottom, left, right};
    }
}

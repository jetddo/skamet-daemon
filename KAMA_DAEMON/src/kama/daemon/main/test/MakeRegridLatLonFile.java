package kama.daemon.main.test;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.FloatBuffer;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;

import kama.daemon.common.util.DaemonUtils;
import kama.daemon.common.util.model.BoundLonLat;
import kama.daemon.common.util.model.ModelGridUtil;

public class MakeRegridLatLonFile {

	public static void main(String[] args) {
		
		try {
			
			Configurations configs = new Configurations();
			
			Configuration config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			String coordinatesLatPath = config.getString("kim_rdps.coordinates.lat.path");
			String coordinatesLonPath = config.getString("kim_rdps.coordinates.lon.path");
			
			ModelGridUtil modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KIM_RDPS, null, coordinatesLatPath, coordinatesLonPath);
			
			double[] mapBound = new double[]{80, -80, 0, 360};
			
			modelGridUtil.setMultipleGridBoundInfoforDistanceGrid(mapBound);
			
			FloatBuffer latitudeBuffer = modelGridUtil.getLatBuffer();
			FloatBuffer longitudeBuffer = modelGridUtil.getLonBuffer();
			
			BoundLonLat maxBoundLonLat = modelGridUtil.getBoundLonLat();
			
			double minLat = maxBoundLonLat.getBottom();
			double maxLat = maxBoundLonLat.getTop();
			double minLon = maxBoundLonLat.getLeft();
			double maxLon = maxBoundLonLat.getRight();
			
			int rows = modelGridUtil.getRows();
			int cols = modelGridUtil.getCols();
			
			double latTerm = (maxLat - minLat) / (rows-1);
			double lonTerm = (maxLon - minLon) / (cols-1);
			
			System.out.println("minLat : " + minLat);
			System.out.println("maxLat : " + maxLat);
			System.out.println("minLon : " + minLon);
			System.out.println("maxLon : " + maxLon);
			System.out.println("latTerm : " + latTerm);
			System.out.println("lonTerm : " + lonTerm);

			
			for(int j=0 ; j<rows ; j++) {
				
				for(int k=0 ; k<cols ; k++) {
					
					float originLat = latitudeBuffer.get(j * cols + k);
					float originLon = longitudeBuffer.get(j * cols + k);
					
					int y = (int)((originLat - minLat) / latTerm);
					int x = (int)((originLon - minLon) / lonTerm);
				}
			}
			
			DataOutputStream dos1 = new DataOutputStream(new FileOutputStream("C:/Users/User/Desktop/kim_rdps_regrid_lat.bin"));
			DataOutputStream dos2 = new DataOutputStream(new FileOutputStream("C:/Users/User/Desktop/kim_rdps_regrid_lon.bin"));
			
			for(int j=0 ; j<rows ; j++) {
				
				for(int k=0 ; k<cols ; k++) {
					
					double regridLat = minLat + latTerm * j;
					double regridLon = minLon + lonTerm * k;
					
					dos1.writeFloat((float)regridLat);
					dos2.writeFloat((float)regridLon);
				}
			}
			
			dos1.close();
			dos2.close();

			
		} catch (Exception e) {
			e.printStackTrace();
		}  finally {

		}
	}
	
}

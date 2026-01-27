package kama.daemon.main.test;

import java.io.BufferedWriter;
import java.io.FileWriter;

import kama.daemon.common.util.model.ModelBinaryExtractor;
import kama.daemon.common.util.model.ModelGridUtil;

public class ModelBinaryExtractorTest {
	
	
	
	public static void main(String[] args) {
		
		ModelBinaryExtractorTest radarTest = new ModelBinaryExtractorTest();
		
		String coordinatesLatPath = "C:/DEV/KAMA_AAMI/workspace/KAMA_DAEMON/res/ldps_regrid_lat.bin";
		String coordinatesLonPath = "C:/DEV/KAMA_AAMI/workspace/KAMA_DAEMON/res/ldps_regrid_lon.bin";
		
		ModelGridUtil modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.LDPS_REGRID, ModelGridUtil.Position.MIDDLE_CENTER, coordinatesLatPath, coordinatesLonPath);
		
		//top=49.01400375366211, bottom=25.214826565570384, left=106.03793334960938, right=147.39876815423105
		
		double[] mapBound = new double[]{80, -80, 0, 360};
		
		modelGridUtil.setMultipleGridBoundInfoforLatLonGrid(mapBound);
		
		modelGridUtil.setSingleGridBoundInfoforLatLonGrid(123, 37);
		
		System.out.println(modelGridUtil.getBoundXY());
		
		System.out.println(modelGridUtil.getPointXY(123, 37));
		
		
		ModelBinaryExtractor modelBinaryExtractor = new ModelBinaryExtractor();
//		float[][] values = modelBinaryExtractor.extractGridsFloat("F:/data/datastore/UM_LOA_PC_BIN/2023/11/01/00/um_loa_pc_regrid_temp_0_0.bin", modelGridUtil, true);
//		
//		try {
//		
//			System.out.println(values.length);
//			System.out.println(values[0].length);
//		
//		    BufferedWriter writer = new BufferedWriter(new FileWriter("F:/data/extract_test/1.txt"));
//		    
//		    for(int i=0 ; i<values.length ; i++) {
//		    	
//		    	for(int j=0 ; j<values[i].length ; j++) {	
//
//		    		if(values[i][j] > 0) {
//		    			writer.write("O");	
//		    		} else {
//		    			writer.write("X");
//		    		}
//		    	}
//		    	
//		    	writer.newLine();
//		    }
//		    
//		    writer.close();
//			
//		} catch (Exception e) {
//			
//		}
	}

}

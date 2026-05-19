package kama.daemon.main.test;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.LineNumberReader;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.model.BoundLonLat;
import kama.daemon.common.util.model.BoundXY;
import kama.daemon.common.util.model.GridCalcUtil;
import kama.daemon.common.util.model.ModelGridUtil;
import kama.daemon.common.util.model.legendfilter.KimRdpsLegendFilter;
import net.coobird.thumbnailator.Thumbnails;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class KimRdpsNe57RegridImageGeneratorTest {
	
	
	private ModelGridUtil modelGridUtil;
	
	private final int imageExpandFactor = 50;
	private final int imageResizeFactor = 2;
	
	private String varName = "RH";
	
	private Map<String, String> regridInfo = null;
	
	private FloatBuffer latitudeBuffer;
	private FloatBuffer longitudeBuffer;
	
	public KimRdpsNe57RegridImageGeneratorTest() {
		
			
		this.initCoordinates();
		
		this.readRegridInfo();
	}

	private void initCoordinates() {
		
		System.out.println("KimRdpsImageGenerator [ Initailize Coordinate Systems ]");
		
		String coordinatesLatPath = "F:/data/datastore/grid/kim_rdps_ne57_lat.bin";
		String coordinatesLonPath = "F:/data/datastore/grid/kim_rdps_ne57_lon.bin";
		
		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KIM_RDPS_NE57, null, coordinatesLatPath, coordinatesLonPath);
				
		this.latitudeBuffer = modelGridUtil.getLatBuffer();
		this.longitudeBuffer = modelGridUtil.getLonBuffer();
	}
	
	private void readRegridInfo() {
		
		System.out.println("\t-> Read KimRdps Regrid Info");
		
		this.regridInfo = new HashMap<String, String>();
		
		File regridInfoFile = new File(String.format("%s/%s", DaemonSettings.getCurrentWorkingDirectory(), "res/kim_rdps_ne57_regrid_info.txt"));
		
		try {
			
			LineNumberReader reader = new LineNumberReader(new FileReader(regridInfoFile));
			
			String line = "";
			
			while((line = reader.readLine()) != null) {
				
				String key = line.split("\\|")[0];
				String value = line.split("\\|")[1];
				
				this.regridInfo.put(key, value);
			}
			
			reader.close();
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void generateImages(NetcdfDataset ncFile, String fileName, String savePath) {

		System.out.println("KimRdpsImageGenerator [ Start Create Tile Images ]");		
		System.out.println("\t-> Create KimRdps Image Grid List");
		
		double[] mapBound = new double[]{50, 20, 110, 150};
		
		this.modelGridUtil.setMultipleGridBoundInfoforDistanceGrid(mapBound);
		
		System.out.println(this.modelGridUtil.getBoundXY());
		
		System.out.println("\t-> Create Image Bound [" + mapBound[0] + ", " + mapBound[1] + ", " + mapBound[2] + ", " + mapBound[3] + "]");
		BoundLonLat boundLonLat = this.generateImage(ncFile, fileName, savePath);
		
		System.out.println("KimRdpsImageGenerator [ End Create Images ]");
	}
	
	private Map<String, Object> getRegridData(float[][] values, BoundLonLat maxBoundLonLat, int rows, int cols) {
		
		System.out.println("\t\t-> Start Regriding");
		
		double minLat = maxBoundLonLat.getBottom();
		double maxLat = maxBoundLonLat.getTop();
		double minLon = maxBoundLonLat.getLeft();
		double maxLon = maxBoundLonLat.getRight();
		
		double latTerm = (maxLat - minLat) / (rows-1);
		double lonTerm = (maxLon - minLon) / (cols-1);
		
		Float[][] regridValues = new Float[rows][];
		boolean[][] regridChecks = new boolean[rows][];
		
		for(int j=0 ; j<rows ; j++) {
			
			regridValues[j] = new Float[cols];
			regridChecks[j] = new boolean[cols];
			
			for(int k=0 ; k<cols ; k++) {
				regridValues[j][k] = -999f;
				regridChecks[j][k] = false;
			}
		}
		
		for(int j=0 ; j<rows ; j++) {
			
			for(int k=0 ; k<cols ; k++) {
				
				float originLat = this.latitudeBuffer.get(j * cols + k);
				float originLon = this.longitudeBuffer.get(j * cols + k);
				
				int y = (int)((originLat - minLat) / latTerm);
				int x = (int)((originLon - minLon) / lonTerm);
				
				regridValues[y][x] = values[j][k];
				regridChecks[y][x] = true;
			}
		}
		
		for(int j=0 ; j<rows ; j++) {
			
			for(int k=0 ; k<cols ; k++) {
				
				if(regridChecks[j][k] == false) {
					
					double regridLat = minLat + latTerm * j;
					double regridLon = minLon + lonTerm * k;
					
					String[] regridInfoTokens = this.regridInfo.get(regridLat + "," + regridLon).split(",");
					
					float originLat = Float.valueOf(regridInfoTokens[0]);
					float originLon = Float.valueOf(regridInfoTokens[1]);
					int boundTop = Integer.valueOf(regridInfoTokens[2]);
					int boundLeft = Integer.valueOf(regridInfoTokens[3]);
					
					if(Math.abs(originLat - regridLat) < latTerm*3 && Math.abs(originLon - regridLon) < lonTerm*3) {
						regridValues[j][k] = values[boundTop][boundLeft];
					}
				}
			}
		}
		
		
		Map<String, Object> regridData = new HashMap<String, Object>();
		regridData.put("regridValues", regridValues);
		regridData.put("latTerm", latTerm);
		regridData.put("lonTerm", lonTerm);
		
		System.out.println("\t\t-> End Regriding");
		
		return regridData;
	}
	
	public BoundLonLat generateImage(NetcdfDataset ncFile, String fileName, String savePath) {
		
		BoundLonLat boundLonLat = null;
		BoundXY boundXY = null;
		
		try {
			
			boundLonLat = this.modelGridUtil.getBoundLonLat();
			boundXY = this.modelGridUtil.getBoundXY();
				
			KimRdpsLegendFilter kimRdpsLegendFilter = new KimRdpsLegendFilter();
	
			int imgHeight = (int)Math.floor((boundLonLat.getTop() - boundLonLat.getBottom()) * this.imageExpandFactor * this.imageResizeFactor); 		    			
			int imgWidth = (int)Math.floor((boundLonLat.getRight() - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
		
			System.out.println("\t-> " + boundLonLat + ", " + boundXY);
			
			int rows = this.modelGridUtil.getRows();
			int cols = this.modelGridUtil.getCols();
				
			double[] mercatorRatio = GridCalcUtil.calculateCumulativeArr(GridCalcUtil.getLatitudeRatioList(boundLonLat.getTop(), boundLonLat.getBottom(), imgHeight, imgHeight));
			
			System.out.println("\t-> Process Attribute [Temperature]");
			
			for(int j=0 ; j<1 ; j++) {
					
				System.out.println("\t\t-> Start Read Variable [Temperature_isobaric]");
				
				Variable var = ncFile.findVariable(this.varName);
					
				List<Range> rangeList = new ArrayList<Range>();
				rangeList.add(new Range(0, 0));		
				rangeList.add(new Range(j, j));						
				rangeList.add(new Range(modelGridUtil.getModelHeight() - boundXY.getTop() - 1, modelGridUtil.getModelHeight() - boundXY.getBottom() - 1));
				rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
			
				float[][] values = GridCalcUtil.convertStorageToPrimitiveValuesFromAttr(var, var.read(rangeList).getStorage(), rows, cols);
				
				System.out.println("\t\t-> End Read Variable [" + this.varName + "]");
				
				Map<String, Object> regridData = getRegridData(values, boundLonLat, rows, cols);
				
				Float[][] regridValues = (Float[][])regridData.get("regridValues");
				
				double latTerm = (double)regridData.get("latTerm");
				double lonTerm = (double)regridData.get("lonTerm");
				
				String imgFileName = fileName.replace(".nc", "") + "_" + String.format("%03d", j+1) + ".png";
					
				File imageFile = new File(savePath + File.separator + imgFileName);
				
				BufferedImage bi = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
				
				Graphics2D ig2 = bi.createGraphics();
				
				System.out.println("\t\t-> Start Write Image [" + imageFile.getAbsolutePath() + "]");
					
				for(int k=0 ; k<rows-1 ; k++) {
					
					for(int l=0 ; l<cols-1 ; l++) {
						
						float v = regridValues[k][l];
						
						Color c = kimRdpsLegendFilter.getColor_Relative_humidity_isobaric(v);
						
						if(c != null) {
							
							double[] xCoords = new double[]{
								boundLonLat.getLeft() + l * lonTerm,
								boundLonLat.getLeft() + (l+1) * lonTerm,
								boundLonLat.getLeft() + (l+1) * lonTerm,
								boundLonLat.getLeft() + l * lonTerm
							};
							
							double[] yCoords = new double[]{
								boundLonLat.getTop() - k * latTerm,
								boundLonLat.getTop() - k * latTerm,
								boundLonLat.getTop() - (k+1) * latTerm,
								boundLonLat.getTop() - (k+1) * latTerm
							};
						
							int[] xPoints = new int[4];
							int[] yPoints = new int[4];
							
							for(int m=0 ; m<4 ; m++) {
								xPoints[m] = (int)Math.floor((xCoords[m] - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
								yPoints[m] = (int)Math.floor(mercatorRatio[(int)Math.floor((yCoords[m] - boundLonLat.getBottom()) * this.imageExpandFactor * this.imageResizeFactor)]);								
							}
							
							ig2.setColor(c);
							ig2.fillPolygon(xPoints, yPoints, 4);
						}
					}
				}
				
				bi = Thumbnails.of(bi).imageType(BufferedImage.TYPE_INT_ARGB).size(imgWidth / this.imageResizeFactor, imgHeight / this.imageResizeFactor).asBufferedImage();
				ImageIO.write(bi, "PNG", imageFile);
				System.out.println("\t\t-> End Write Image [" + imageFile.getAbsolutePath() + "]");
			}
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return boundLonLat;
	}
	
	public static void main(String[] args) {
        
		KimRdpsNe57RegridImageGeneratorTest kimRdpsImageGeneratorTest = new KimRdpsNe57RegridImageGeneratorTest();
                  
		try {
						
			NetcdfDataset ncFile = NetcdfDataset.acquireDataset("F:/KAMA_AAMI/2026/항기청_수신/항기청_수신_20260514/r030_v040_easia_prs.2byte.ft000.2026050100.nc", null);
			String fileName = "r030_v040_easia_prs.2byte.ft000.2026050100.nc";
	        String savePath = "F:/data/kim_rdps_img";
	        
	        kimRdpsImageGeneratorTest.generateImages(ncFile, fileName, savePath);
	        
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
	
}

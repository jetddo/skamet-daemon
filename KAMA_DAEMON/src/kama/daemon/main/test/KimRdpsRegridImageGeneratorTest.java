package kama.daemon.main.test;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import kama.daemon.common.util.model.BoundLonLat;
import kama.daemon.common.util.model.BoundXY;
import kama.daemon.common.util.model.GridCalcUtil;
import kama.daemon.common.util.model.ModelGridUtil;
import kama.daemon.common.util.model.legendfilter.KimRdpsLegendFilter;
import net.coobird.thumbnailator.Thumbnails;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class KimRdpsRegridImageGeneratorTest {
	
	
	private ModelGridUtil modelGridUtil;
	
	private final int imageExpandFactor = 50;
	private final int imageResizeFactor = 2;
	
	private String varName = "Temperature_isobaric";
	
	private FloatBuffer latitudeBuffer;
	private FloatBuffer longitudeBuffer;
	
	public KimRdpsRegridImageGeneratorTest() {
		
			
		this.initCoordinates();
	}

	private void initCoordinates() {
		
		System.out.println("KimRdpsImageGenerator [ Initailize Coordinate Systems ]");
		
		String coordinatesLatPath = "F:/data/datastore/grid/kim_rdps_lat.bin";
		String coordinatesLonPath = "F:/data/datastore/grid/kim_rdps_lon.bin";
		
		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KIM_RDPS, null, coordinatesLatPath, coordinatesLonPath);
				
		this.latitudeBuffer = modelGridUtil.getLatBuffer();
		this.longitudeBuffer = modelGridUtil.getLonBuffer();
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
	
	private Map<String, Object> getRegridData(Float[][] values, BoundLonLat maxBoundLonLat, int rows, int cols) {
		
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
					
					//if(regridLat >= this.cropBottom && regridLat <= this.cropTop && regridLon >= this.cropLeft && regridLon <= this.cropRight) {
						
						this.modelGridUtil.setSingleGridBoundInfoforDistanceGrid(regridLon, regridLat);
						
						BoundLonLat boundLonLat = this.modelGridUtil.getBoundLonLat();
						BoundXY boundXY = this.modelGridUtil.getBoundXY();
						
						float originLat = (float)boundLonLat.getTop();
						float originLon = (float)boundLonLat.getLeft();
						
						if(Math.abs(originLat - regridLat) < latTerm*3 && Math.abs(originLon - regridLon) < lonTerm*3) {
							regridValues[j][k] = values[boundXY.getTop()][boundXY.getLeft()];
						}	
					//}
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
				rangeList.add(new Range(23-j, 23-j));						
				rangeList.add(new Range(modelGridUtil.getModelHeight() - boundXY.getTop() - 1, modelGridUtil.getModelHeight() - boundXY.getBottom() - 1));
				rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
			
				Float[][] values = GridCalcUtil.convertStorageToValues(var.read(rangeList).getStorage(), rows, cols);
				
				System.out.println("\t\t-> End Read Variable [" + this.varName + "]");
				
				Map<String, Object> regridData = getRegridData(values, boundLonLat, rows, cols);
				
				Float[][] regridValues = (Float[][])regridData.get("regridValues");
				
				double latTerm = (double)regridData.get("latTerm");
				double lonTerm = (double)regridData.get("lonTerm");
				
				String imgFileName = fileName.replace(".gb2", "") + "_" + String.format("%03d", j+1) + ".png";
					
				File imageFile = new File(savePath + File.separator + imgFileName);
				
				BufferedImage bi = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
				
				Graphics2D ig2 = bi.createGraphics();
				
				System.out.println("\t\t-> Start Write Image [" + imageFile.getAbsolutePath() + "]");
					
				for(int k=0 ; k<rows-1 ; k++) {
					
					for(int l=0 ; l<cols-1 ; l++) {
						
						float v = regridValues[k][l];
						
						Color c = (Color)kimRdpsLegendFilter.getClass().getMethod("getColor_" + this.varName, double.class).invoke(kimRdpsLegendFilter, v);
						
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
        
		KimRdpsRegridImageGeneratorTest kimRdpsImageGeneratorTest = new KimRdpsRegridImageGeneratorTest();
                  
		try {
						
			NetcdfDataset ncFile = NetcdfDataset.acquireDataset("F:/data/datastore/KIM_RDPS/2025/04/20/00/r030_v040_ne36_pres_h000.2025042000.gb2", null);
			String fileName = "r030_v040_ne36_pres_h000.2025042000.gb2";
	        String savePath = "F:/data/kim_rdps_img";
	        
	        kimRdpsImageGeneratorTest.generateImages(ncFile, fileName, savePath);
	        
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
	
}

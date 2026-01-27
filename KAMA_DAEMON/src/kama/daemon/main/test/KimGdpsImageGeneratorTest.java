package kama.daemon.main.test;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.imageio.ImageIO;

import kama.daemon.common.util.model.BoundLonLat;
import kama.daemon.common.util.model.BoundXY;
import kama.daemon.common.util.model.GridCalcUtil;
import kama.daemon.common.util.model.ModelGridUtil;
import kama.daemon.common.util.model.legendfilter.KimGdpsLegendFilter;
import net.coobird.thumbnailator.Thumbnails;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class KimGdpsImageGeneratorTest {
	
	
	private ModelGridUtil modelGridUtil;
	
	private final int imageExpandFactor = 50;
	private final int imageResizeFactor = 2;
	
	private String varName = "Temperature_isobaric";
	
	public KimGdpsImageGeneratorTest() {
		
			
		this.initCoordinates();
	}

	private void initCoordinates() {
		
		System.out.println("KimGdpsImageGenerator [ Initailize Coordinate Systems ]");
		
		String coordinatesLatPath = "F:/data/datastore/grid/kim_gdps_lat.bin";
		String coordinatesLonPath = "F:/data/datastore/grid/kim_gdps_lon.bin";
		
		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KIM_GDPS, ModelGridUtil.Position.MIDDLE_CENTER, coordinatesLatPath, coordinatesLonPath);
	}
	
	public void generateImages(NetcdfDataset ncFile, String fileName, String savePath) {

		System.out.println("KimGdpsImageGenerator [ Start Create Tile Images ]");		
		System.out.println("\t-> Create KimGdps Image Grid List");
		
		double[] mapBound = new double[]{50, 20, 110, 150};
		
		this.modelGridUtil.setMultipleGridBoundInfoforLatLonGrid(mapBound);
		
		System.out.println(this.modelGridUtil.getBoundXY());
		
		System.out.println("\t-> Create Image Bound [" + mapBound[0] + ", " + mapBound[1] + ", " + mapBound[2] + ", " + mapBound[3] + "]");
		BoundLonLat boundLonLat = this.generateImage(ncFile, fileName, savePath);
		
		System.out.println("KimGdpsImageGenerator [ End Create Images ]");
	}
	
	public BoundLonLat generateImage(NetcdfDataset ncFile, String fileName, String savePath) {
		
		BoundLonLat boundLonLat = null;
		BoundXY boundXY = null;
		
		try {
			
			boundLonLat = this.modelGridUtil.getBoundLonLat();
			boundXY = this.modelGridUtil.getBoundXY();
			
			System.out.println(boundLonLat);
				
			KimGdpsLegendFilter kimGdpsLegendFilter = new KimGdpsLegendFilter();
	
			int imgHeight = (int)Math.floor((boundLonLat.getTop() - boundLonLat.getBottom()) * this.imageExpandFactor * this.imageResizeFactor); 		    			
			int imgWidth = (int)Math.floor((boundLonLat.getRight() - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
		
			System.out.println("\t-> " + boundLonLat + ", " + boundXY);
			
			int rows = this.modelGridUtil.getRows();
			int cols = this.modelGridUtil.getCols();
			
			double[] latInterval = GridCalcUtil.calculateCumulativeArr(this.modelGridUtil.getLatInterval());
			double[] lonInterval = GridCalcUtil.calculateCumulativeArr(this.modelGridUtil.getLonInterval());
			
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
			
				Float[][] values = GridCalcUtil.convertStorageToValuesReverse(var.read(rangeList).getStorage(), rows, cols);
				
				System.out.println("\t\t-> End Read Variable [" + this.varName + "]");
				
				String imgFileName = fileName.replace(".gb2", "") + "_" + String.format("%03d", j+1) + ".png";
					
				File imageFile = new File(savePath + File.separator + imgFileName);
				
				BufferedImage bi = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
				
				Graphics2D ig2 = bi.createGraphics();
				
				System.out.println("\t\t-> Start Write Image [" + imageFile.getAbsolutePath() + "]");
					
				for(int k=0 ; k<rows ; k++) {
					
					for(int l=0 ; l<cols ; l++) {
						
						float v = values[k][l];
						
						Color c = (Color)kimGdpsLegendFilter.getClass().getMethod("getColor_" + this.varName, double.class).invoke(kimGdpsLegendFilter, v);
						
						if(c != null) {
							
							double[] xCoords = new double[]{
								boundLonLat.getLeft() + lonInterval[l],
								boundLonLat.getLeft() + lonInterval[l+1],
								boundLonLat.getLeft() + lonInterval[l+1],
								boundLonLat.getLeft() + lonInterval[l]
							};
							
							double[] yCoords = new double[]{
								boundLonLat.getTop() - latInterval[k],
								boundLonLat.getTop() - latInterval[k],
								boundLonLat.getTop() - latInterval[k+1],
								boundLonLat.getTop() - latInterval[k+1]
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
        
		KimGdpsImageGeneratorTest kimGdpsImageGeneratorTest = new KimGdpsImageGeneratorTest();
                  
		try {
						
			NetcdfDataset ncFile = NetcdfDataset.acquireDataset("F:/data/datastore/KIM_GDPS/2023/07/05/00/kim_g128_ne36_pres_h000.2023070500.gb2", null);
			String fileName = "kim_g128_ne36_pres_h000.2023070500.gb2";
	        String savePath = "F:/data/kim_gdps_img";
	        
	        kimGdpsImageGeneratorTest.generateImages(ncFile, fileName, savePath);
	        
			
			
		} catch (Exception e) {
			
		}
		
		
	}
	
}

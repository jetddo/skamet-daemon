package kama.daemon.common.util.model.image;

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

import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.model.BoundLonLat;
import kama.daemon.common.util.model.BoundXY;
import kama.daemon.common.util.model.GridCalcUtil;
import kama.daemon.common.util.model.ModelGridUtil;
import kama.daemon.common.util.model.legendfilter.LdpsLegendFilter;

import org.apache.commons.lang3.ArrayUtils;

import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class LdpsRegridImageGenerator {
	
	private DaemonSettings daemonSettings;
	private ProcessorInfo processorInfo;
	private DatabaseManager dbManager;
	
	private ModelGridUtil modelGridUtil;
	
	private FloatBuffer latitudeBuffer;
	private FloatBuffer longitudeBuffer;
	
	private final int imageExpandFactor = 40;
	private final int imageResizeFactor = 1;
	
	private String attrName;
	private String varName;	
	
	public LdpsRegridImageGenerator(DatabaseManager dbManager, ProcessorInfo processorInfo, String attrName, String varName) {
		
		this.daemonSettings = dbManager.Settings;
		this.processorInfo = processorInfo;
		this.dbManager = dbManager;
		
		this.attrName = attrName;
		this.varName = varName;
			
		this.initCoordinates();
	}

	private void initCoordinates() {
		
		System.out.println("LdpsImageGenerator [ Initailize Coordinate Systems ]");
		
		String coordinatesLatPath = this.daemonSettings.getCoordinatesLatPath();
		String coordinatesLonPath = this.daemonSettings.getCoordinatesLonPath();
		
		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.LDPS, null, coordinatesLatPath, coordinatesLonPath);
		
		this.latitudeBuffer = modelGridUtil.getLatBuffer();
		this.longitudeBuffer = modelGridUtil.getLonBuffer();
	}
	
	public void generateImages(NetcdfDataset ncFile, String fileName, String savePath) {

		System.out.println("LdpsImageGenerator [ Start Create Tile Images ]");		
		System.out.println("\t-> Create Ldps Image Grid List");
		
//		double[] mapBound = new double[]{40, 30, 0, 360};
		double[] mapBound = new double[]{80, -80, 0, 360};
		
		this.modelGridUtil.setMultipleGridBoundInfoforDistanceGrid(mapBound);
		
		System.out.println("\t-> Create Image Bound [" + mapBound[0] + ", " + mapBound[1] + ", " + mapBound[2] + ", " + mapBound[3] + "]");
		BoundLonLat boundLonLat = this.generateImage(ncFile, fileName, savePath);
		
		System.out.println("LdpsImageGenerator [ End Create Image ]");
	}
	
	private Map<String, Object> getRegridData(Float[][] values, BoundLonLat maxBoundLonLat, int rows, int cols) {
		
		System.out.println("\t\t-> Start Regriding [" + this.varName + "]");
		
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
					
					this.modelGridUtil.setSingleGridBoundInfoforDistanceGrid(regridLon, regridLat);
					
					BoundLonLat boundLonLat = this.modelGridUtil.getBoundLonLat();
					BoundXY boundXY = this.modelGridUtil.getBoundXY();
					
					float originLat = (float)boundLonLat.getTop();
					float originLon = (float)boundLonLat.getLeft();
					
					if(Math.abs(originLat - regridLat) < latTerm*3 && Math.abs(originLon - regridLon) < lonTerm*3) {
						regridValues[j][k] = values[boundXY.getTop()][boundXY.getLeft()];
					}
				}
			}
		}
		
		Map<String, Object> regridData = new HashMap<String, Object>();
		regridData.put("regridValues", regridValues);
		regridData.put("latTerm", latTerm);
		regridData.put("lonTerm", lonTerm);
		
		System.out.println("\t\t-> End Regriding [" + this.varName + "]");
		
		return regridData;
	}
	
	public BoundLonLat generateImage(NetcdfDataset ncFile, String fileName, String savePath) {
		
		BoundLonLat boundLonLat = null;
		BoundXY boundXY = null;
		
		try {
			
			boundLonLat = this.modelGridUtil.getBoundLonLat();			
			boundXY = this.modelGridUtil.getBoundXY();
	
			LdpsLegendFilter legendFilter = new LdpsLegendFilter();
	
			int imgHeight = (int)Math.floor((boundLonLat.getTop() - boundLonLat.getBottom()) * this.imageExpandFactor * this.imageResizeFactor); 		    			
			int imgWidth = (int)Math.floor((boundLonLat.getRight() - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
		
			System.out.println("\t-> " + boundLonLat + ", " + boundXY);
			
			int rows = this.modelGridUtil.getRows();
			int cols = this.modelGridUtil.getCols();
			
			double[] mercatorRatio = GridCalcUtil.calculateCumulativeArr(GridCalcUtil.getLatitudeRatioList(boundLonLat.getTop(), boundLonLat.getBottom(), imgHeight, imgHeight));
			
			System.out.println("\t-> Process Attribute [" + this.attrName + "]");
			
			int timeIndex = Integer.valueOf(fileName.replaceAll("qwumloa_pc", "").replaceAll(".nc", ""));
			
			int timeLength = fileName.contains("pc000") ? 1 : 2;
			
			for(int i=0 ; i<timeLength ; i++) {
				
				for(int j=0 ; j<1 ; j++) {
					
					System.out.println("\t\t-> Start Read Variable [" + this.varName + "]");
					
					Variable var = ncFile.findVariable(this.varName);
					
					List<Range> rangeList = new ArrayList<Range>();
					rangeList.add(new Range(i, i));	
					rangeList.add(new Range(j, j));						
					rangeList.add(new Range(modelGridUtil.getModelHeight() - boundXY.getTop() - 1, modelGridUtil.getModelHeight() - boundXY.getBottom() - 1));
					rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
				
					Float[][] values = GridCalcUtil.convertStorageToValues(var.read(rangeList).getStorage(), rows, cols);
						
					System.out.println("\t\t-> End Read Variable [" + this.varName + "]");
					
					Map<String, Object> regridData = getRegridData(values, boundLonLat, rows, cols);
					
					Float[][] regridValues = (Float[][])regridData.get("regridValues");
					double latTerm = (double)regridData.get("latTerm");
					double lonTerm = (double)regridData.get("lonTerm");
						
					String imgFileName = fileName.replaceAll("pc[0-9]{3}.nc", "") + "pc" + String.format("%03d", timeIndex - (timeLength-1) + i) + "_" + String.format("%02d", j) +".png";
					
					File imageFile = new File(savePath + File.separator + imgFileName);
					
					BufferedImage bi = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
					
					Graphics2D ig2 = bi.createGraphics();
					
					System.out.println("\t\t-> Start Write Image [" + imageFile.getAbsolutePath() + "]");
					
					for(int k=0 ; k<rows-1 ; k++) {
						
						for(int l=0 ; l<cols-1 ; l++) {
							
							float v = regridValues[k][l];
							
							Color c = (Color)legendFilter.getClass().getMethod("getColor_" + this.varName, double.class).invoke(legendFilter, v);
							
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
						
					ImageIO.write(bi, "PNG", imageFile);
					System.out.println("\t\t-> End Write Image [" + imageFile.getAbsolutePath() + "]");
				}
			}
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return boundLonLat;
	}
	    
    private double[] getLatitudeRatioList(double top, double bottom, int y, int scaleFactorY) {
    	
    	double total = 0;
    	List<Double> ratioList = new ArrayList<Double>();
    	
    	for(int i=0 ; i<=y ; i++) {
    		
    		ratioList.add(Math.abs(1 / Math.cos((top - (i * (top - bottom) / y)) * Math.PI / 180)));
    		total += ratioList.get(i);
    	}
    	
    	for(int i=0 ; i<ratioList.size() ; i++) {
    		ratioList.set(i, ratioList.get(i) / total * scaleFactorY);
    	}
    	
    	return ArrayUtils.toPrimitive(ratioList.toArray(new Double[ratioList.size()]));
    }
}

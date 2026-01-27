package kama.daemon.common.util.model.image;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.imageio.ImageIO;

import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.db.struct.ProcessorInfo;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.model.BoundLonLat;
import kama.daemon.common.util.model.BoundXY;
import kama.daemon.common.util.model.GridCalcUtil;
import kama.daemon.common.util.model.ModelGridUtil;
import kama.daemon.common.util.model.legendfilter.GktgArcvLegendFilter;
import net.coobird.thumbnailator.Thumbnails;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class GktgArcvImageGenerator {
	
	private DaemonSettings daemonSettings;
	private ProcessorInfo processorInfo;
	private DatabaseManager dbManager;
	
	private ModelGridUtil modelGridUtil;
	
	private final int imageExpandFactor = 30;
	private final int imageResizeFactor = 2;
	
	public GktgArcvImageGenerator(DatabaseManager dbManager, ProcessorInfo processorInfo) {
		
		this.daemonSettings = dbManager.Settings;
		this.processorInfo = processorInfo;
		this.dbManager = dbManager;
			
		this.initCoordinates();
	}

	private void initCoordinates() {
		
		System.out.println("GktgArcvImageGenerator [ Initailize Coordinate Systems ]");
		
		String coordinatesLatPath = this.daemonSettings.getCoordinatesLatPath();
		String coordinatesLonPath = this.daemonSettings.getCoordinatesLonPath();
		
		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.GKTG_ARCV, ModelGridUtil.Position.MIDDLE_CENTER, coordinatesLatPath, coordinatesLonPath);
	}
	
	public void generateImages(NetcdfDataset ncFile, String fileName, String varName, String savePath) {

		System.out.println("GktgArcvImageGenerator [ Start Create Tile Images ]");		
		System.out.println("\t-> Create GktgArcv Image Grid List");
		
		double[] mapBound = new double[]{50, 20, 110, 150};
		
		this.modelGridUtil.setMultipleGridBoundInfoforLatLonGrid(mapBound);
		
		System.out.println(this.modelGridUtil.getBoundXY());
		
		System.out.println("\t-> Create Image Bound [" + mapBound[0] + ", " + mapBound[1] + ", " + mapBound[2] + ", " + mapBound[3] + "]");
		BoundLonLat boundLonLat = this.generateImage(ncFile, fileName, varName, savePath);
		
		System.out.println("GktgArcvImageGenerator [ End Create Images ]");
	}
	
	public BoundLonLat generateImage(NetcdfDataset ncFile, String fileName, String varName, String savePath) {
		
		BoundLonLat boundLonLat = null;
		BoundXY boundXY = null;
		
		try {
			
			boundLonLat = this.modelGridUtil.getBoundLonLat();
			boundXY = this.modelGridUtil.getBoundXY();
			
			System.out.println(boundLonLat);
				
			GktgArcvLegendFilter gktgArcvLegendFilter = new GktgArcvLegendFilter();
	
			int imgHeight = (int)Math.floor((boundLonLat.getTop() - boundLonLat.getBottom()) * this.imageExpandFactor * this.imageResizeFactor); 		    			
			int imgWidth = (int)Math.floor((boundLonLat.getRight() - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
		
			System.out.println("\t-> " + boundLonLat + ", " + boundXY);
			
			int rows = this.modelGridUtil.getRows();
			int cols = this.modelGridUtil.getCols();
			
			double[] latInterval = GridCalcUtil.calculateCumulativeArr(this.modelGridUtil.getLatInterval());
			double[] lonInterval = GridCalcUtil.calculateCumulativeArr(this.modelGridUtil.getLonInterval());
			
			double[] mercatorRatio = GridCalcUtil.calculateCumulativeArr(GridCalcUtil.getLatitudeRatioList(boundLonLat.getTop(), boundLonLat.getBottom(), imgHeight, imgHeight));
			
			System.out.println("\t-> Process Attribute [GTGMAX]");
			
			for(int j=0 ; j<40 ; j++) {
					
				System.out.println("\t\t-> Start Read Variable ["+varName+"]");
				
				Variable var = ncFile.findVariable(varName);
					
				List<Range> rangeList = new ArrayList<Range>();
				rangeList.add(new Range(j, j));						
				rangeList.add(new Range(modelGridUtil.getModelHeight() - boundXY.getTop() - 1, modelGridUtil.getModelHeight() - boundXY.getBottom() - 1));
				rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
			
				Float[][] values = GridCalcUtil.convertStorageToValues(var.read(rangeList).getStorage(), rows, cols);
				
				System.out.println("\t\t-> End Read Variable [" + varName + "]");
				
				String imgFileName = fileName.replace(".nc", "") + "_" + varName.toLowerCase() + "_" + String.format("%03d", j+1) + ".png";
					
				File imageFile = new File(savePath + File.separator + imgFileName);
				
				BufferedImage bi = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
				
				Graphics2D ig2 = bi.createGraphics();
				
				System.out.println("\t\t-> Start Write Image [" + imageFile.getAbsolutePath() + "]");
					
				for(int k=0 ; k<rows ; k++) {
					
					for(int l=0 ; l<cols ; l++) {
						
						float v = values[k][l];
						
						Color c = (Color)gktgArcvLegendFilter.getClass().getMethod("getColor_" + varName, double.class).invoke(gktgArcvLegendFilter, v);
						
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
}

package kama.daemon.common.util.model.image;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.ArrayList;
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
import kama.daemon.common.util.model.legendfilter.KtgDiagIndexLegendFilter;
import net.coobird.thumbnailator.Thumbnails;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class KtgDiagIndxImageGenerator {
	
	private DaemonSettings daemonSettings;
	private ProcessorInfo processorInfo;
	private DatabaseManager dbManager;
	
	private ModelGridUtil modelGridUtil;
	
	private final int imageExpandFactor = 20;
	private final int imageResizeFactor = 2;
	
	final String[] varNames = {"def", "pvu", "lapse", "div", "vws"};
	
	public KtgDiagIndxImageGenerator(DatabaseManager dbManager, ProcessorInfo processorInfo) {
		
		this.daemonSettings = dbManager.Settings;
		this.processorInfo = processorInfo;
		this.dbManager = dbManager;
			
		this.initCoordinates();
	}

	private void initCoordinates() {
		
		System.out.println("KtgDiagIndexmageGenerator [ Initailize Coordinate Systems ]");
		
		String coordinatesLatPath = this.daemonSettings.getCoordinatesLatPath();
		String coordinatesLonPath = this.daemonSettings.getCoordinatesLonPath();
		
		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KTG, null, coordinatesLatPath, coordinatesLonPath);
	}
	
	public void generateImages(NetcdfDataset ncFile, String fileName, String savePath) {

		System.out.println("KtgDiagIndexmageGenerator [ Start Create Tile Images ]");		
		System.out.println("\t-> Create KtgDiagIndex Image Grid List");
		
		double[] mapBound = new double[]{80, -80, 0, 360};
		
		this.modelGridUtil.setMultipleGridBoundInfoforDistanceGrid(mapBound);
		
		System.out.println("\t-> Create Image Bound [" + mapBound[0] + ", " + mapBound[1] + ", " + mapBound[2] + ", " + mapBound[3] + "]");
		BoundLonLat boundLonLat = this.generateImage(ncFile, fileName, savePath);
		
		System.out.println("KtgDiagIndexmageGenerator [ End Create Image ]");
	}
	
	public BoundLonLat generateImage(NetcdfDataset ncFile, String fileName, String savePath) {
		
		BoundLonLat boundLonLat = null;
		BoundXY boundXY = null;
		
		try {
			
			boundLonLat = this.modelGridUtil.getBoundLonLat();			
			boundXY = this.modelGridUtil.getBoundXY();
			
			List<Map<String, Object>> polygonDataList = this.modelGridUtil.getPolygonDataList();
	
			KtgDiagIndexLegendFilter ktgDiagIndexLegendFilter = new KtgDiagIndexLegendFilter();
	
			int imgHeight = (int)Math.floor((boundLonLat.getTop() - boundLonLat.getBottom()) * this.imageExpandFactor * this.imageResizeFactor); 		    			
			int imgWidth = (int)Math.floor((boundLonLat.getRight() - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
		
			System.out.println("\t-> " + boundLonLat + ", " + boundXY);
			
			int rows = this.modelGridUtil.getRows();
			int cols = this.modelGridUtil.getCols();
			
			double[] mercatorRatio = GridCalcUtil.calculateCumulativeArr(GridCalcUtil.getLatitudeRatioList(boundLonLat.getTop(), boundLonLat.getBottom(), imgHeight, imgHeight));
			
			for (int i = 0; i < this.varNames.length; i++) {

				System.out.println("\t-> Process Attribute [" + this.varNames[i] + "]");
				
				int startIndex = fileName.contains("amo_gdum_ktgm_midl") ? 0 : 3;
				int endIndex = fileName.contains("amo_gdum_ktgm_midl") ? 22 : 23;
				
				for(int j=startIndex ; j<endIndex ; j++) {
						
					System.out.println("\t\t-> Start Read Variable [" + this.varNames[i] + "]");
					
					Variable var = ncFile.findVariable(this.varNames[i]);
					
					List<Range> rangeList = new ArrayList<Range>();
					rangeList.add(new Range(j, j));						
					rangeList.add(new Range(modelGridUtil.getModelHeight() - boundXY.getTop() - 1, modelGridUtil.getModelHeight() - boundXY.getBottom() - 1));
					rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
				
					Float[][] values = GridCalcUtil.convertStorageToValues(var.read(rangeList).getStorage(), rows, cols);
					
					System.out.println("\t\t-> End Read Variable [" + this.varNames[i] + "]");
					
					String imgFileName = fileName.replace(".nc", "") + "_" + this.varNames[i] + "_" + String.format("%03d", j+1) + ".png";
					
					File imageFile = new File(savePath + File.separator + imgFileName);
					
					BufferedImage bi = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
					
					Graphics2D ig2 = bi.createGraphics();
					
					System.out.println("\t\t-> Start Write Image [" + imageFile.getAbsolutePath() + "]");
					
					for(int k=0 ; k<polygonDataList.size() ; k++) {
						
						Map<String, Object> polygonData = polygonDataList.get(k);
						
						int x = Integer.valueOf(polygonData.get("x").toString());
						int y = Integer.valueOf(polygonData.get("y").toString());
						
						Color c = (Color)ktgDiagIndexLegendFilter.getClass().getMethod("getColor_" + this.varNames[i], double.class).invoke(ktgDiagIndexLegendFilter, values[y][x]);

						if(c == null) {
							continue;
						}
						
						double[][] coordinates = (double[][])polygonData.get("coordinates");
							
						int[] xPoints = new int[coordinates.length];
						int[] yPoints = new int[coordinates.length];
						
						for(int m=0 ; m<coordinates.length ; m++) {						
							xPoints[m] = (int)Math.floor((coordinates[m][0] - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
							yPoints[m] = (int)Math.floor(mercatorRatio[(int)Math.floor((boundLonLat.getTop() - coordinates[m][1]) * this.imageExpandFactor * this.imageResizeFactor)]);
						}
						
						ig2.setColor(c);
						ig2.fillPolygon(xPoints, yPoints, coordinates.length);
					}
					
					bi = Thumbnails.of(bi).imageType(BufferedImage.TYPE_INT_ARGB).size(imgWidth / this.imageResizeFactor, imgHeight / this.imageResizeFactor).asBufferedImage();
					ImageIO.write(bi, "PNG", imageFile);
					System.out.println("\t\t-> End Write Image [" + imageFile.getAbsolutePath() + "]");
				}
			}
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return boundLonLat;
	}
}

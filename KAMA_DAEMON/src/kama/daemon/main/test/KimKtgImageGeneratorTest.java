package kama.daemon.main.test;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import kama.daemon.common.util.model.BoundLonLat;
import kama.daemon.common.util.model.BoundXY;
import kama.daemon.common.util.model.GridCalcUtil;
import kama.daemon.common.util.model.ModelGridUtil;
import kama.daemon.common.util.model.legendfilter.KimKtgLegendFilter;
import net.coobird.thumbnailator.Thumbnails;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class KimKtgImageGeneratorTest {
	
	private ModelGridUtil modelGridUtil;
	
	private final int imageExpandFactor = 20;
	private final int imageResizeFactor = 2;
	
	public KimKtgImageGeneratorTest() {		
		
		this.initCoordinates();
	}

	private void initCoordinates() {
		
		System.out.println("KimKtgImageGenerator [ Initailize Coordinate Systems ]");
		
		String coordinatesLatPath = "F:/data/datastore/grid/kim_ktg_lat.bin";
		String coordinatesLonPath = "F:/data/datastore/grid/kim_ktg_lon.bin";
		
		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KIM_KTG, null, coordinatesLatPath, coordinatesLonPath);
	}
	
	public void generateImages(NetcdfDataset ncFile, String fileName, String savePath) {

		System.out.println("KimKtgImageGenerator [ Start Create Tile Images ]");		
		System.out.println("\t-> Create KimKtg Image Grid List");
		
		double[] mapBound = new double[]{80, -80, 0, 360};
		
		this.modelGridUtil.setMultipleGridBoundInfoforDistanceGrid(mapBound);
		
		System.out.println("\t-> Create Image Bound [" + mapBound[0] + ", " + mapBound[1] + ", " + mapBound[2] + ", " + mapBound[3] + "]");
		BoundLonLat boundLonLat = this.generateImage(ncFile, fileName, savePath);
		
		System.out.println("KimKtgImageGenerator [ End Create Image ]");
	}
	
	public BoundLonLat generateImage(NetcdfDataset ncFile, String fileName, String savePath) {
		
		BoundLonLat boundLonLat = null;
		BoundXY boundXY = null;
		
		try {
			
			boundLonLat = this.modelGridUtil.getBoundLonLat();			
			boundXY = this.modelGridUtil.getBoundXY();
			
			List<Map<String, Object>> polygonDataList = this.modelGridUtil.getPolygonDataList();
	
			KimKtgLegendFilter kimKtgLegendFilter = new KimKtgLegendFilter();
	
			int imgHeight = (int)Math.floor((boundLonLat.getTop() - boundLonLat.getBottom()) * this.imageExpandFactor * this.imageResizeFactor); 		    			
			int imgWidth = (int)Math.floor((boundLonLat.getRight() - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
		
			System.out.println("\t-> " + boundLonLat + ", " + boundXY);
			
			int rows = this.modelGridUtil.getRows();
			int cols = this.modelGridUtil.getCols();
			
			double[] mercatorRatio = GridCalcUtil.calculateCumulativeArr(GridCalcUtil.getLatitudeRatioList(boundLonLat.getTop(), boundLonLat.getBottom(), imgHeight, imgHeight));
	
			System.out.println("\t-> Process Attribute [KTG_AUC_20]");
			
			int startIndex = fileName.contains("amo_kimg_ktgm_midl") ? 0 : 3;
			int endIndex = fileName.contains("amo_kimg_ktgm_midl") ? 22 : 23;
			
			for(int j=startIndex ; j<endIndex ; j++) {
					
				System.out.println("\t\t-> Start Read Variable [KTG_AUC_20]");
				
				Variable var = ncFile.findVariable("KTG_AUC_20");
				
				List<Range> rangeList = new ArrayList<Range>();
				rangeList.add(new Range(j, j));						
				rangeList.add(new Range(modelGridUtil.getModelHeight() - boundXY.getTop() - 1, modelGridUtil.getModelHeight() - boundXY.getBottom() - 1));
				rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
			
				Float[][] values = GridCalcUtil.convertStorageToValues(var.read(rangeList).getStorage(), rows, cols);
				
				System.out.println("\t\t-> End Read Variable [KTG_AUC_20]");
				
				String imgFileName = fileName.replace(".nc", "") + "_" + String.format("%03d", j + 1) + ".png";
				
				File imageFile = new File(savePath + File.separator + imgFileName);
				
				BufferedImage bi = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
				
				Graphics2D ig2 = bi.createGraphics();
				
				System.out.println("\t\t-> Start Write Image [" + imageFile.getAbsolutePath() + "]");
				
				for(int k=0 ; k<polygonDataList.size() ; k++) {
					
					Map<String, Object> polygonData = polygonDataList.get(k);
					
					int x = Integer.valueOf(polygonData.get("x").toString());
					int y = Integer.valueOf(polygonData.get("y").toString());
					
					Color c = kimKtgLegendFilter.getColor_KTG_AUC_20(values[y][x]);

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
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return boundLonLat;
	}
	
	public static void main(String[] args) {
        
		KimKtgImageGeneratorTest test = new KimKtgImageGeneratorTest();
                  
		try {
						
			NetcdfDataset ncFile = NetcdfDataset.acquireDataset("F:/KAMA_AAMI/2026/항기청_수신/항기청_수신_20260318/amo_kimg_ktgm_midl_f00_2026031800.nc", null);
			String fileName = "amo_kimg_ktgm_midl_f00_2026031800.nc";
	        String savePath = "F:/data/kim_ktg_img";
	        
	        test.generateImages(ncFile, fileName, savePath);
	        
			
			
		} catch (Exception e) {
			
		}
		
		
	}
}

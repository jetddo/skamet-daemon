package kama.daemon.main;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FilenameFilter;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import kama.daemon.common.db.DatabaseManager;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DaemonUtils;
import kama.daemon.common.util.model.BoundLonLat;
import kama.daemon.common.util.model.BoundXY;
import kama.daemon.common.util.model.GridCalcUtil;
import kama.daemon.common.util.model.ModelGridUtil;
import kama.daemon.common.util.model.legendfilter.KimKfipLegendFilter;
import net.coobird.thumbnailator.Thumbnails;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class MakeKimKfipImage {
	
	private ModelGridUtil modelGridUtil;
	
	private final int imageExpandFactor = 30;
	private final int imageResizeFactor = 2;
	
	private String varName = "KIMFIP";
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private final String insertFileProcInfo = 
			
			" INSERT INTO AAMI.STORED_FILE_PROC_H(FILE_DT, FILE_NAME, FILE_PATH, PROC_DT, FILE_CD) VALUES " + 
			" (TO_DATE('{fileDt}', 'YYYYMMDDHH24MI'), '{fileName}', '{filePath}', SYSDATE, 'KIM_KFIP') "; 
	
	private final String selectFileProcInfoList = 
			
			" SELECT 												"+
			" 	TO_CHAR((FILE_DT), 'YYYYMMDDHH24') AS FILE_DT,		"+
			" 	FILE_NAME											"+
			" FROM AAMI.STORED_FILE_PROC_H							"+
			" WHERE FILE_DT >= TO_DATE('{targetDt}', 'YYYYMMDD')	"+
			" AND FILE_CD = 'KIM_KFIP'								";
		
	private Configuration config;
	
	private DatabaseManager dbManager;
	
	private boolean initialize() {
		
		Configurations configs = new Configurations();
		
		try {
		
			this.config = configs.properties(new File(DaemonUtils.getConfigFilePath()));
			
			this.dbManager = DatabaseManager.getInstance();
			this.dbManager.setConfig(new DaemonSettings(this.config));
			this.dbManager.setAutoCommit(false);
				
		} catch (ConfigurationException e ) {
			
			System.out.println("Error : MakeKimKfipImage.initialize -> " + e);
			
			this.dbManager.safeClose();
			
			return false;
		}
		
		this.initCoordinates();
		
		return true;
	}
	
	private void initCoordinates() {
		
		System.out.println("MakeKimKfipImage [ Initailize Coordinate Systems ]");
		
		String latPath = config.getString("kim_kfip.coordinates.lat.path");
		String lonPath = config.getString("kim_kfip.coordinates.lon.path");
		
		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KIM_KFIP, ModelGridUtil.Position.MIDDLE_CENTER, latPath, lonPath);
		
		System.out.println("MakeKimKfipImage [ Start Create Tile Images ]");		
		System.out.println("\t-> Create KimKfip Image Grid List");
		
		double[] mapBound = new double[]{50, 5, 90, 160};
		
		this.modelGridUtil.setMultipleGridBoundInfoforLatLonGrid(mapBound);
		
		System.out.println(this.modelGridUtil.getBoundXY());
		
		System.out.println("\t-> Create Image Bound [" + mapBound[0] + ", " + mapBound[1] + ", " + mapBound[2] + ", " + mapBound[3] + "]");
		
	}
	
	private void destroy() {
		this.dbManager.safeClose();
	}
	
	private void process() {
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: Start Initialize :::::");
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHH");
		SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy/MM/dd/HH"); // 날짜 형식만 필요할 때 사용	
		
		if(!this.initialize()) {
			
			System.out.println("Error : MakeKimKfipImage.process -> initialize failed");
			return;
		}
		
		final String kimKfipFilter = this.config.getString("kim_kfip.filter");
		
		String storePath = DaemonUtils.isWindow() ? this.config.getString("global.storePath.windows") 
												  : this.config.getString("global.storePath.unix");
		
		//storePath = "\\\\172.26.56.124\\data_store";
		
		try {
						
			Calendar cal = new GregorianCalendar();
						
			cal.setTime(new Date());				
			cal.add(Calendar.HOUR_OF_DAY, -9-12);
			cal.add(Calendar.HOUR_OF_DAY, -cal.get(Calendar.HOUR_OF_DAY)%6); // 6시간 단위로 맞추기 위해 현재 시간을 6시간 단위로 내림 처리
			// 하루전꺼부터 체크
			
			String targetDtStr = sdf.format(cal.getTime());
			
			String query = this.selectFileProcInfoList.replaceAll("\\{targetDt\\}", targetDtStr);	
			
			List<Map<String, Object>> parsedFileInfoList = new ArrayList<Map<String, Object>>();
			
			ResultSet resultSet = dbManager.executeQuery(query);
			
			while(resultSet.next()) {
				
				Map<String, Object> parsedFileInfo = DaemonUtils.getCamelcaseResultSetData(resultSet);				
				parsedFileInfoList.add(parsedFileInfo);
			}
			
			for(int i=0 ; i<3 ; i++) {
				
				Date targetDt = cal.getTime();			
				
				String targetDirStr = storePath + File.separator + "/KIM_KFIP/" + sdf3.format(targetDt);
				
				File targetDir = new File(targetDirStr);
				
				System.out.print("Target Dir : " + targetDir.getAbsolutePath());
			
				if(targetDir.exists()) {
					
					System.out.println(" -> exist, checked");
					
					File[] kimKfipFiles = targetDir.listFiles(new FilenameFilter(){

						@Override
						public boolean accept(File dir, String name) {
							
							if(name.matches(kimKfipFilter) && !name.endsWith("png")) {
								return true;
							} 
							
							return false;
						}
					});
					
					
					List<File> kimKfipFileList = new ArrayList<File>();
					
					for(File kimKfipFile : kimKfipFiles) {
						
						boolean isParsed = false;
						
						for(int j=0 ; j<parsedFileInfoList.size() ; j++) {
							
							Map<String, Object> parsedFileInfo = parsedFileInfoList.get(j);
							
							Date fileDt = sdf2.parse(parsedFileInfo.get("fileDt").toString());
							String fileName = parsedFileInfo.get("fileName").toString();
							
							if(kimKfipFile.getName().equals(fileName)) {
								isParsed = true;
							}
						}
						
						if(!isParsed) {
							kimKfipFileList.add(kimKfipFile);			
						}
					}
					
					kimKfipFileList.sort(new Comparator<File>() {

						@Override
						public int compare(File arg0, File arg1) {							
							return arg0.getName().compareTo(arg1.getName());
						}
					});
					
					this.drawKimKfipImages(kimKfipFileList, storePath);
					
				} else {
					System.out.println(" -> not exist");
				}
				
				cal.add(Calendar.HOUR_OF_DAY, 6);	
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		this.destroy();
		
		System.out.println(this.logDateFormat.format(new Date(System.currentTimeMillis())) + " -> ::::: End :::::");
	}
	
	public void drawKimKfipImages(List<File> kimKfipFileList, String storePath) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHH");
		SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy/MM/dd/HH"); // 날짜 형식만 필요할 때 사용	
				
		System.out.println("MakeKimKfipImage [ Start Create Images ]");		
	
		for(int i=0 ; i<kimKfipFileList.size() ; i++) {
			
			File kimKfipFile = kimKfipFileList.get(i);
			
			System.out.println("Target File : " + kimKfipFile.getAbsolutePath());
			
			try {
				
				NetcdfDataset ncFile = NetcdfDataset.acquireDataset(kimKfipFile.getAbsolutePath(), null);
				String fileName = kimKfipFile.getName();
				
				Date fileDt = sdf2.parse(fileName.split("_")[5].split("\\.")[0]);
				
		        String savePath = storePath + "/KFIP_ASIA_IMG/" + sdf3.format(fileDt) ;
		        
		        File saveDir = new File(savePath);
		        
		        if(!saveDir.exists()) {
		        	saveDir.mkdirs();
		        }		      
		        
		        this.generateImage(ncFile, fileName, savePath);		 
		        
				String query = this.insertFileProcInfo.replaceAll("\\{fileDt\\}", sdf2.format(fileDt))
						  							  .replaceAll("\\{fileName\\}", fileName)
						  							  .replaceAll("\\{filePath\\}", kimKfipFile.getAbsolutePath());
				
				this.dbManager.executeQuery(query);
				this.dbManager.commit();
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		System.out.println("MakeKimKfipImage [ End Create Images ]");
	}
	
	public BoundLonLat generateImage(NetcdfDataset ncFile, String fileName, String savePath) {

		BoundLonLat boundLonLat = null;
		BoundXY boundXY = null;
		
		try {
			
			boundLonLat = this.modelGridUtil.getBoundLonLat();
			boundXY = this.modelGridUtil.getBoundXY();
			
			System.out.println(boundLonLat);
				
			KimKfipLegendFilter kimKfipLegendFilter = new KimKfipLegendFilter();
	
			int imgHeight = (int)Math.floor((boundLonLat.getTop() - boundLonLat.getBottom()) * this.imageExpandFactor * this.imageResizeFactor); 		    			
			int imgWidth = (int)Math.floor((boundLonLat.getRight() - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
		
			System.out.println("\t-> " + boundLonLat + ", " + boundXY);
			
			int rows = this.modelGridUtil.getRows();
			int cols = this.modelGridUtil.getCols();
			
			double[] latInterval = GridCalcUtil.calculateCumulativeArr(this.modelGridUtil.getLatInterval());
			double[] lonInterval = GridCalcUtil.calculateCumulativeArr(this.modelGridUtil.getLonInterval());
			
			double[] mercatorRatio = GridCalcUtil.calculateCumulativeArr(GridCalcUtil.getLatitudeRatioList(boundLonLat.getTop(), boundLonLat.getBottom(), imgHeight, imgHeight));
			
			System.out.println("\t-> Process Attribute ["+varName+"]");
			
			Variable var = ncFile.findVariable(this.varName);
			
			int[][][] preXPoints = new int[rows][cols][4];
			int[][][] preYPoints = new int[rows][cols][4];

			double leftLon = boundLonLat.getLeft();
			double topLat = boundLonLat.getTop();
			double bottomLat = boundLonLat.getBottom();
			double scale = this.imageExpandFactor * this.imageResizeFactor;

			for(int k=0 ; k<rows ; k++) {
			    for(int l=0 ; l<cols ; l++) {

			        double x0 = leftLon + lonInterval[l];
			        double x1 = leftLon + lonInterval[l + 1];
			        double y0 = topLat - latInterval[k];
			        double y1 = topLat - latInterval[k + 1];

			        preXPoints[k][l][0] = (int)Math.floor((x0 - leftLon) * scale);
			        preXPoints[k][l][1] = (int)Math.floor((x1 - leftLon) * scale);
			        preXPoints[k][l][2] = (int)Math.floor((x1 - leftLon) * scale);
			        preXPoints[k][l][3] = (int)Math.floor((x0 - leftLon) * scale);

			        preYPoints[k][l][0] = (int)Math.floor(mercatorRatio[(int)Math.floor((y0 - bottomLat) * scale)]);
			        preYPoints[k][l][1] = (int)Math.floor(mercatorRatio[(int)Math.floor((y0 - bottomLat) * scale)]);
			        preYPoints[k][l][2] = (int)Math.floor(mercatorRatio[(int)Math.floor((y1 - bottomLat) * scale)]);
			        preYPoints[k][l][3] = (int)Math.floor(mercatorRatio[(int)Math.floor((y1 - bottomLat) * scale)]);
			    }
			}
			
			for(int j=0 ; j<40 ; j++) {
					
				System.out.println("\t\t-> Start Read Variable [" + this.varName + "]");
					
				List<Range> rangeList = new ArrayList<Range>();
				rangeList.add(new Range(j, j));						
				rangeList.add(new Range(modelGridUtil.getModelHeight() - boundXY.getTop() - 1, modelGridUtil.getModelHeight() - boundXY.getBottom() - 1));
				rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
			
				Float[][] values = GridCalcUtil.convertStorageToValues(var.read(rangeList).getStorage(), rows, cols);
				
				System.out.println("\t\t-> End Read Variable [" + this.varName + "]");
				
				String imgFileName = fileName.replace(".nc", "") + "_" + String.format("%03d", j+1) + ".png";
					
				File imageFile = new File(savePath + File.separator + imgFileName);
				
				BufferedImage bi = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
				
				Graphics2D ig2 = bi.createGraphics();
				
				System.out.println("\t\t-> Start Write Image [" + imageFile.getAbsolutePath() + "]");
					
				for(int k=0 ; k<rows ; k++) {
					
					for(int l=0 ; l<cols ; l++) {
						
						float v = values[k][l];
						
						Color c = kimKfipLegendFilter.getColor_KIMFIP(v);
						
						if(c != null) {
						    ig2.setColor(c);
						    ig2.fillPolygon(preXPoints[k][l], preYPoints[k][l], 4);
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

		new MakeKimKfipImage().process();
	}
}
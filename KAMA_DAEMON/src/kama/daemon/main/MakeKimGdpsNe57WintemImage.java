package kama.daemon.main;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
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
import net.coobird.thumbnailator.Thumbnails;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class MakeKimGdpsNe57WintemImage {
	
	private ModelGridUtil modelGridUtil;
	
	private final int imageExpandFactor = 50;
	private final int imageResizeFactor = 2;
	
	private static BufferedImage[][] wtailImageList;
	
	private Map<String, String> paramMap = null;
	
	private float wTailRatio = 1f;
	private int wTailWidth = (int)(this.imageExpandFactor * this.imageResizeFactor * this.wTailRatio);
	private int wTailHeight = (int)(this.imageExpandFactor * this.imageResizeFactor * this.wTailRatio);
	private int fontSize = (int)(this.imageExpandFactor * this.imageResizeFactor / 4);
	
	private final String fontName = "Times New Roman";
	
	private final String wintemImgFilePattern = "WINTEM_KIM_GDPS_{weatherType}_FL{heightText}_{fcstHour}H_{issuedDt}.jpg";

	private List<Map<String, Object>> fileInfoList = new ArrayList<Map<String, Object>>();
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private final String insertFileProcInfo = 
			
			" INSERT INTO AAMI.STORED_FILE_PROC_H(FILE_DT, FILE_NAME, FILE_PATH, PROC_DT, FILE_CD) VALUES " + 
			" (TO_DATE('{fileDt}', 'YYYYMMDDHH24MI'), '{fileName}', '{filePath}', SYSDATE, 'KIM_GDPS_NE57_WINTEM') "; 
	
	private final String selectFileProcInfoList = 
			
			" SELECT 												"+
			" 	TO_CHAR((FILE_DT), 'YYYYMMDDHH24') AS FILE_DT,		"+
			" 	FILE_NAME											"+
			" FROM AAMI.STORED_FILE_PROC_H							"+
			" WHERE FILE_DT >= TO_DATE('{targetDt}', 'YYYYMMDD')	"+
			" AND FILE_CD = 'KIM_GDPS_NE57_WINTEM'					";
		
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
			
			System.out.println("Error : MakeKimGdpsNe57WintemImage.initialize -> " + e);
			
			this.dbManager.safeClose();
			
			return false;
		}
		
		this.initCoordinates();
		
		return true;
	}
	
	private void initCoordinates() {
		
		System.out.println("MakeKimGdpsNe57WintemImage [ Initailize Coordinate Systems ]");
		
		String latPath = config.getString("kim_gktg.coordinates.lat.path");
		String lonPath = config.getString("kim_gktg.coordinates.lon.path");
		
		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KIM_GKTG, ModelGridUtil.Position.MIDDLE_CENTER, latPath, lonPath);
		
		System.out.println("MakeKimGdpsNe57WintemImage [ Start Create Tile Images ]");		
		System.out.println("\t-> Create KimGdps Image Grid List");
		
		double[] mapBound = new double[]{50, 20, 110, 150};
		
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
			
			System.out.println("Error : MakeKimGdpsNe57WintemImage.process -> initialize failed");
			return;
		}
		
		final String kimGdpsFilter = "g576_v091_glob_prs.2byte.ft[0-9]{3}.[0-9]{8}00.nc";
		
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
				
				String targetDirStr = storePath + File.separator + "/KIM_GDPS_NE57/" + sdf3.format(targetDt);
				
				File targetDir = new File(targetDirStr);
				
				System.out.print("Target Dir : " + targetDir.getAbsolutePath());
			
				if(targetDir.exists()) {
					
					System.out.println(" -> exist, checked");
					
					File[] kimGdpsFiles = targetDir.listFiles(new FilenameFilter(){

						@Override
						public boolean accept(File dir, String name) {
							
							if(name.matches(kimGdpsFilter)) {
								return true;
							} 
							
							return false;
						}
					});
					
					
					List<File> kimGdpsFileList = new ArrayList<File>();
					
					for(File kimGdpsFile : kimGdpsFiles) {
						
						boolean isParsed = false;
						
						for(int j=0 ; j<parsedFileInfoList.size() ; j++) {
							
							Map<String, Object> parsedFileInfo = parsedFileInfoList.get(j);
							
							Date fileDt = sdf2.parse(parsedFileInfo.get("fileDt").toString());
							String fileName = parsedFileInfo.get("fileName").toString();
							
							if(kimGdpsFile.getName().equals(fileName)) {
								isParsed = true;
							}
						}
						
						if(!isParsed) {
							kimGdpsFileList.add(kimGdpsFile);			
						}
					}
					
					kimGdpsFileList.sort(new Comparator<File>() {

						@Override
						public int compare(File arg0, File arg1) {							
							return arg0.getName().compareTo(arg1.getName());
						}
					});
					
					this.drawWintemImages(kimGdpsFileList, storePath);
					
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
	
	public void drawWintemImages(List<File> kimGdpsFileList, String storePath) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHH");
		SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy/MM/dd/HH"); // 날짜 형식만 필요할 때 사용	
				
		System.out.println("MakeKimGdpsNe57WintemImage [ Start Create Images ]");		
		
		for(int i=0 ; i<kimGdpsFileList.size() ; i++) {
			
			File kimGdpsFile = kimGdpsFileList.get(i);
			
			System.out.println("Target File : " + kimGdpsFile.getAbsolutePath());
			
			try {
				
				NetcdfDataset ncFile = NetcdfDataset.acquireDataset(kimGdpsFile.getAbsolutePath(), null);
				String fileName = kimGdpsFile.getName();
				
				Date fileDt = sdf2.parse(fileName.split("\\.")[3]);
				
		        String savePath = storePath + "/KIM_GDPS_NE57_WINTEM/" + sdf3.format(fileDt) ;
		        
		        File saveDir = new File(savePath);
		        
		        if(!saveDir.exists()) {
		        	saveDir.mkdirs();
		        }
				
				// 평시상태 이미지 생성
		        Map<String, String> paramMap1 = new HashMap<String, String>();
		        paramMap1.put("cropTop", "39");
		        paramMap1.put("cropBottom", "27.5");
		        paramMap1.put("cropLeft", "121");
		        paramMap1.put("cropRight", "135");
		        paramMap1.put("imgWidth", "720");
		        paramMap1.put("imgHeight", "720");
		        paramMap1.put("weatherType", "NOR");
		        paramMap1.put("wintemBaseImg", "wintem_base2_upscaled.png");
		        
		        this.generateImage(ncFile, fileName, savePath, paramMap1);		
		        
//		        // 위기상태 이미지 생성
//		        Map<String, String> paramMap2 = new HashMap<String, String>();
//		        paramMap2.put("cropTop", "44");
//		        paramMap2.put("cropBottom", "27.5");
//		        paramMap2.put("cropLeft", "119");
//		        paramMap2.put("cropRight", "135");
//		        paramMap2.put("imgWidth", "720");
//		        paramMap2.put("imgHeight", "900");	  
//		        paramMap2.put("weatherType", "WRN");
//		        paramMap2.put("wintemBaseImg", "wintem_base3_upscaled.png");
//		        
//		        this.generateImage(ncFile, fileName, savePath, paramMap2);		
		        
				String query = this.insertFileProcInfo.replaceAll("\\{fileDt\\}", sdf2.format(fileDt))
						  							  .replaceAll("\\{fileName\\}", fileName)
						  							  .replaceAll("\\{filePath\\}", kimGdpsFile.getAbsolutePath());
				
				//this.dbManager.executeQuery(query);
				this.dbManager.commit();
				
				ncFile.close();
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		System.out.println("MakeKimGdpsNe57WintemImage [ End Create Images ]");
	}
	
	public BoundLonLat generateImage(NetcdfDataset ncFile, String fileName, String savePath, Map<String, String> paramMap) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmm");

		this.fileInfoList.clear();
		
		BoundLonLat boundLonLat = null;
		BoundXY boundXY = null;
		
		double cropTop = Double.parseDouble(paramMap.get("cropTop"));
		double cropBottom = Double.parseDouble(paramMap.get("cropBottom"));
		double cropLeft = Double.parseDouble(paramMap.get("cropLeft"));
		double cropRight = Double.parseDouble(paramMap.get("cropRight"));
		int resizeImgWidth = Integer.parseInt(paramMap.get("imgWidth"));
		int resizeImgHeight = Integer.parseInt(paramMap.get("imgHeight"));
		String weatherType = paramMap.get("weatherType");
		String wintemBaseImgFileName = paramMap.get("wintemBaseImg");
		
		try {
			
			this.createWtailImages();
			
			boundLonLat = this.modelGridUtil.getBoundLonLat();
			boundXY = this.modelGridUtil.getBoundXY();
	
			int imgHeight = (int)Math.floor((boundLonLat.getTop() - boundLonLat.getBottom()) * this.imageExpandFactor * this.imageResizeFactor); 		    			
			int imgWidth = (int)Math.floor((boundLonLat.getRight() - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
		
			System.out.println("\t-> " + boundLonLat + ", " + boundXY);
			
			int rows = this.modelGridUtil.getRows();
			int cols = this.modelGridUtil.getCols();
			
			double[] latInterval = GridCalcUtil.calculateCumulativeArr(this.modelGridUtil.getLatInterval());
			double[] lonInterval = GridCalcUtil.calculateCumulativeArr(this.modelGridUtil.getLonInterval());
			
			double[] mercatorRatio = GridCalcUtil.calculateCumulativeArr(GridCalcUtil.getLatitudeRatioList(boundLonLat.getTop(), boundLonLat.getBottom(), imgHeight, imgHeight));
			
			String[] savePaths = savePath.split("\\/");
			
			Calendar cal = new GregorianCalendar();
			cal.set(Calendar.YEAR, Integer.valueOf(savePaths[savePaths.length-4]));
			cal.set(Calendar.MONTH, Integer.valueOf(savePaths[savePaths.length-3])-1);
			cal.set(Calendar.DAY_OF_MONTH, Integer.valueOf(savePaths[savePaths.length-2]));
			cal.set(Calendar.HOUR_OF_DAY, Integer.valueOf(savePaths[savePaths.length-1]));
			
			Date issuedDt = sdf.parse(fileName.split("\\.")[3]);
			
			int fcstHour = Integer.valueOf(fileName.split("\\.")[2].replaceAll("ft", ""));
			
			System.out.println("\t-> Model Fcst Hour: +" + fcstHour + "H");
			
			if(fcstHour > 36) {
				return boundLonLat;
			}
			
			cal.add(Calendar.HOUR_OF_DAY, fcstHour);
			
			Date fcstDt = cal.getTime();
			
			System.out.println("\t-> Process Create Image");
			
			List<Map<String, Object>> wintemUseDataInfoList =  this.getWintemUseDataInfoList();
			
			for(int i=0 ; i<wintemUseDataInfoList.size() ; i++) {
				
				Map<String, Object> wintemUseDataInfo = wintemUseDataInfoList.get(i);
				
				String heightIndexes = wintemUseDataInfo.get("heightIndexes").toString();
				String heightText = wintemUseDataInfo.get("heightText").toString();
				
				Map<String, float[][]> wintemDataSet = this.getWintemDataSet(ncFile, rows, cols, boundXY, heightIndexes);
				
				float[][] valuesUWind = wintemDataSet.get("valuesUWind");
				float[][] valuesVWind = wintemDataSet.get("valuesVWind");
				float[][] valuesTemp = wintemDataSet.get("valuesTemp");
				
				String imgFileName = this.wintemImgFilePattern.toString();
				imgFileName = imgFileName.replaceAll("\\{weatherType\\}", weatherType);
				imgFileName = imgFileName.replaceAll("\\{heightText\\}", heightText);
				imgFileName = imgFileName.replaceAll("\\{fcstHour\\}", String.format("%02d", fcstHour));
				imgFileName = imgFileName.replaceAll("\\{issuedDt\\}", new SimpleDateFormat("yyyyMMddHHmm").format(issuedDt));
					
				Map<String, Object> fileInfo = new HashMap<String, Object>();
				
				this.fileInfoList.add(fileInfo);
				
				File imageFile = new File(savePath + File.separator + imgFileName);
				
				fileInfo.put("imageFile", imageFile);
				
				BufferedImage bi = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
				
				Graphics2D ig2 = bi.createGraphics();
				
				ig2.setFont(new Font(this.fontName, Font.BOLD, this.fontSize));
				ig2.setColor(new Color(0, 0, 255));	
				
				System.out.println("\t\t-> Start Write Image [" + imageFile.getAbsolutePath() + "]");
					
				for(int k=0 ; k<rows ; k++) {
					
					for(int l=0 ; l<cols ; l++) {
						
						if(k % 10 != 0 || l % 10 != 0) {
							continue;
						}
						
						float u = valuesUWind[k][l];
						float v = valuesVWind[k][l];
						
						if(u == -999 || v == -999) {
							continue;
						}
							
						double wd = Math.atan2(u, v) * 180 / Math.PI + 180;		
						double ws = Math.sqrt(u*u + v*v);
						int temp = (int)(valuesTemp[k][l] - 273.15);		
							
						int tailIndex = (int)Math.floor(ws * 1.943844 / 5);
						
						if(tailIndex > 19) {
							tailIndex = 19;
						}
						
						double xCoord = boundLonLat.getLeft() + lonInterval[l];							
						double yCoord = boundLonLat.getTop() - latInterval[k];
						
						int xPoint = (int)Math.floor((xCoord - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
						int yPoint = (int)Math.floor(mercatorRatio[(int)Math.floor((yCoord - boundLonLat.getBottom()) * this.imageExpandFactor * this.imageResizeFactor)]);
						
//						ig2.drawImage(wtailImageList[tailIndex][(int)((wd + 360) % 360) / 10], xPoint, yPoint, null);
						ig2.drawImage(wtailImageList[tailIndex][(int)((wd + 360) % 360) / 10], xPoint - this.wTailWidth / 2, yPoint - this.wTailHeight / 2, null);
						
						if((int)((wd + 360) % 360) > 90 && (int)((wd + 360) % 360) < 270) {
							ig2.drawString(temp < 0 ? String.format("%02d", temp * -1) : "PS"+String.format("%02d", temp), temp < 0 ? xPoint - 15 : xPoint - 30, yPoint - 5);	
						} else {
							ig2.drawString(temp < 0 ? String.format("%02d", temp * -1) : "PS"+String.format("%02d", temp), temp < 0 ? xPoint - 15 : xPoint - 30, yPoint - 5 + this.fontSize);	
						}
					}
				}
				
				bi = this.cropImage(bi, boundLonLat, mercatorRatio, cropTop, cropBottom, cropLeft, cropRight);
				
				BufferedImage wintemBaseImg = ImageIO.read(new File(String.format("%s/%s", DaemonSettings.getCurrentWorkingDirectory(), "res/"+wintemBaseImgFileName)));
				
				wintemBaseImg.getGraphics().drawImage(bi, 0, 0, wintemBaseImg.getWidth(), wintemBaseImg.getHeight(), null);
				
				this.appendWintemLabel(wintemBaseImg, issuedDt, fcstDt, heightText, fcstHour);
				
				wintemBaseImg = Thumbnails.of(wintemBaseImg).imageType(BufferedImage.TYPE_INT_ARGB).forceSize(resizeImgWidth, resizeImgHeight).asBufferedImage();
				
				ImageIO.write(wintemBaseImg, "PNG", imageFile);
				
				System.out.println("\t\t-> End Write Image [" + imageFile.getAbsolutePath() + "]");
				
				ig2.dispose();
				ig2 = null;
				///////////////////////////////////////////////////////////////////////////////////
				
			}
		
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		
		return boundLonLat;
    }
	
	private void appendWintemLabel(BufferedImage wintemBaseImg, Date issuedDt, Date fcstDt, String feet, int fcstHour) {

		final int labelWidth = 700;
		final int labelHeight = 320;
		final int lineHeight = 40;
		final int labelTopMargin = 10;		
		final int fontLeftMargin = 10;
		
		SimpleDateFormat labelDateFormat = new SimpleDateFormat("HH \'UTC\' dd MM yyyy"); 
				
		Graphics2D ig2 = wintemBaseImg.createGraphics();
		ig2.setColor(new Color(255, 255, 255));
		ig2.fillRect(wintemBaseImg.getWidth() - labelWidth, wintemBaseImg.getHeight() - labelHeight, labelWidth, labelHeight);
		
		ig2.setStroke(new BasicStroke(3));
		ig2.setColor(new Color(0, 0, 0));
		ig2.drawRect(wintemBaseImg.getWidth() - labelWidth, wintemBaseImg.getHeight() - labelHeight, labelWidth, labelHeight);
		
		ig2.setFont(new Font(fontName, Font.PLAIN, 40));
		ig2.setColor(new Color(0, 0, 0));	
							
		ig2.drawString("ISSUED BY AMO", wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin + lineHeight*1);
		ig2.drawString("WIND/TEMPERATURE FL " + feet, wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin + lineHeight*2);
		ig2.drawString("FIXED TIME PROGNOSTIC CHART", wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin + lineHeight*3);
		
		ig2.drawString("BASED ON " + labelDateFormat.format(issuedDt), wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin + lineHeight*5);
		
		ig2.drawString("VALID " + labelDateFormat.format(fcstDt), wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin + lineHeight*4);
		
		ig2.setFont(new Font(fontName, Font.PLAIN, 30));
		ig2.drawString("Units: Wind(knots), Temperature(℃)", wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin + lineHeight*6);		
		ig2.drawString("Temperatures are negative unless prefixed by PS", wintemBaseImg.getWidth() - labelWidth + fontLeftMargin, wintemBaseImg.getHeight() - labelHeight + labelTopMargin/2 + lineHeight*7);
		
		ig2.dispose();
		ig2 = null;
	}
	
	private BufferedImage cropImage(BufferedImage bi, BoundLonLat boundLonLat, double[] mercatorRatio, double top, double bottom, double left, double right) {
		
		System.out.println(boundLonLat);
		System.out.println("\t-> Crop Image [ top: " + top + ", bottom: " + bottom + ", left: " + left + ", right: " + right + " ]");
		
		int y1 = (int)Math.floor(mercatorRatio[(int)Math.floor((boundLonLat.getTop() - top) * this.imageExpandFactor * this.imageResizeFactor)]);
		int y2 = (int)Math.floor(mercatorRatio[(int)Math.floor((boundLonLat.getTop() - bottom) * this.imageExpandFactor * this.imageResizeFactor)]);
		int x1 = (int)Math.floor((left - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
		int x2 = (int)Math.floor((right - boundLonLat.getLeft()) * this.imageExpandFactor * this.imageResizeFactor);
		
		return bi.getSubimage(x1, y1, x2 - x1, y2 - y1);
	}
	
	private void createWtailImages() throws IOException {
		
		if(wtailImageList != null) {
			System.out.println("\t-> Already Arrow Images Created");
			return;
		} else {
			System.out.println("\t-> Create Arrow Image List");	
		}
		
		String wtailImageDir = String.format("%s/%s", DaemonSettings.getCurrentWorkingDirectory(), "res/wtail/");
		
		String wtailImagePath = wtailImageDir + "/windTail_blue_{ws}/windTail_blue_{ws}_{wd}.png";
		
		wtailImageList = new BufferedImage[20][];
		
		for(int i=0 ; i<wtailImageList.length ; i++) {
			
			wtailImageList[i] = new BufferedImage[36];
			
			for(int j=0 ; j<wtailImageList[i].length ; j++) {
				
				File originImageFile = new File(wtailImagePath.replaceAll("\\{ws\\}", String.valueOf(i+1))
															  .replaceAll("\\{wd\\}", String.valueOf(j*10)));
				
				wtailImageList[i][j] = Thumbnails.of(originImageFile)
						.imageType(BufferedImage.TYPE_INT_ARGB)
						.size(this.wTailWidth, this.wTailHeight).asBufferedImage();
			}
		}
	}
	
	private Map<String, float[][]> getWintemDataSet(NetcdfDataset ncFile, int rows, int cols, BoundXY boundXY, String heightIndexes) throws Exception {
		
		String[] heightIndexList = heightIndexes.split(",");
		
		System.out.println("\t\t-> Start Read Variable [u, v, T]");
		System.out.println("\t\t-> heightIndexes: " + heightIndexes);
		
		List<float[][]> valuesUWindList = new ArrayList<float[][]>();
		List<float[][]> valuesVWindList = new ArrayList<float[][]>();
		List<float[][]> valuesTempList = new ArrayList<float[][]>();
		
		for(int i=0 ; i<heightIndexList.length ; i++) {
			
			String heightIndex = heightIndexList[i];
										
			Variable varUWind = ncFile.findVariable("u");
			Variable varVWind = ncFile.findVariable("v");
			Variable varTemp = ncFile.findVariable("T");
			
			List<Range> rangeList = new ArrayList<Range>();
			rangeList.add(new Range(0, 0));		
			rangeList.add(new Range(Integer.valueOf(heightIndex), Integer.valueOf(heightIndex)));
			rangeList.add(new Range(boundXY.getBottom(), boundXY.getTop()));
			rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));
		
			valuesUWindList.add(GridCalcUtil.convertStorageToPrimitiveValuesFromAttr(varUWind, varUWind.read(rangeList).getStorage(), rows, cols));
			valuesVWindList.add(GridCalcUtil.convertStorageToPrimitiveValuesFromAttr(varVWind, varVWind.read(rangeList).getStorage(), rows, cols));
			valuesTempList.add(GridCalcUtil.convertStorageToPrimitiveValuesFromAttr(varTemp, varTemp.read(rangeList).getStorage(), rows, cols));
		}
		
		Map<String, float[][]> wintemDataSet = new HashMap<String, float[][]>();
		
		if(heightIndexList.length > 1) {
	
			float[][] valuesUWind = new float[rows][cols];
			float[][] valuesVWind = new float[rows][cols];
			float[][] valuesTemp = new float[rows][cols];

			for(int i=0 ; i<rows ; i++) {			
				for(int j=0 ; j<cols ; j++) {				
					
					float u = 0;
					float v = 0;
					float temp = 0;
					
					for(int k=0 ; k<heightIndexList.length ; k++) {
						u += valuesUWindList.get(k)[i][j];
						v += valuesVWindList.get(k)[i][j];
						temp += valuesTempList.get(k)[i][j];
					}
					
					valuesUWind[i][j] = u / (float)(heightIndexList.length);
					valuesVWind[i][j] = v / (float)(heightIndexList.length);
					valuesTemp[i][j] = temp / (float)(heightIndexList.length);
				}
			}
			
			wintemDataSet.put("valuesUWind", valuesUWind);
			wintemDataSet.put("valuesVWind", valuesVWind);
			wintemDataSet.put("valuesTemp", valuesTemp);
			
			
		} else {
			
			wintemDataSet.put("valuesUWind", valuesUWindList.get(0));
			wintemDataSet.put("valuesVWind", valuesVWindList.get(0));
			wintemDataSet.put("valuesTemp", valuesTempList.get(0));
		}
		
		System.out.println("\t\t-> End Read Variable [u, v, T]");
		
		return wintemDataSet;
	}
	
	private List<Map<String, Object>> getWintemUseDataInfoList() {
		
		// 각 고도별 데이터 활용 정보
		List<Map<String, Object>> wintemUseDataInfoList = new ArrayList<Map<String, Object>>();
		
		Map<String, Object> wintemUseDataInfo = null;
		
		// FT6000		
		wintemUseDataInfo = new HashMap<String, Object>();
		wintemUseDataInfo.put("heightIndexes", "0");
		wintemUseDataInfo.put("heightText", "060");
		wintemUseDataInfoList.add(wintemUseDataInfo);
		
		
		return wintemUseDataInfoList;
	}
	
	public static void main(String[] args) {

		new MakeKimGdpsNe57WintemImage().process();
	}
}
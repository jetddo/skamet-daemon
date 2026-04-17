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
import kama.daemon.common.util.model.legendfilter.KimGktgLegendFilter;
import net.coobird.thumbnailator.Thumbnails;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class MakeKimGktgImage {
	
	private ModelGridUtil modelGridUtil;
	
	private final int imageExpandFactor = 10;
	private final int imageResizeFactor = 2;
	
	private SimpleDateFormat logDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	private final String insertFileProcInfo = 
			
			" INSERT INTO AAMI.STORED_FILE_PROC_H(FILE_DT, FILE_NAME, FILE_PATH, PROC_DT, FILE_CD) VALUES " + 
			" (TO_DATE('{fileDt}', 'YYYYMMDDHH24MI'), '{fileName}', '{filePath}', SYSDATE, 'KIM_GKTG') "; 
	
	private final String selectFileProcInfoList = 
			
			" SELECT 												"+
			" 	TO_CHAR((FILE_DT), 'YYYYMMDDHH24') AS FILE_DT,		"+
			" 	FILE_NAME											"+
			" FROM AAMI.STORED_FILE_PROC_H							"+
			" WHERE FILE_DT >= TO_DATE('{targetDt}', 'YYYYMMDD')	"+
			" AND FILE_CD = 'KIM_GKTG'								";
		
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
			
			System.out.println("Error : MakeKimGktgImage.initialize -> " + e);
			
			this.dbManager.safeClose();
			
			return false;
		}
		
		this.initCoordinates();
		
		return true;
	}
	
	private void initCoordinates() {
		
		System.out.println("MakeKimGktgImage [ Initailize Coordinate Systems ]");
		
		String latPath = config.getString("kim_gktg.coordinates.lat.path");
		String lonPath = config.getString("kim_gktg.coordinates.lon.path");
		
		this.modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.KIM_GKTG, ModelGridUtil.Position.MIDDLE_CENTER, latPath, lonPath);
		
		System.out.println("MakeKimGktgImage [ Start Create Tile Images ]");		
		System.out.println("\t-> Create KimGktg Image Grid List");
		
		double[] mapBound = new double[]{80, -80, 0, 360};
		
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
			
			System.out.println("Error : MakeKimGktgImage.process -> initialize failed");
			return;
		}
		
		final String kimGktgFilter = this.config.getString("kim_gktg.filter");
		
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
				
				String targetDirStr = storePath + File.separator + "/KIM_GKTG/" + sdf3.format(targetDt);
				
				File targetDir = new File(targetDirStr);
				
				System.out.print("Target Dir : " + targetDir.getAbsolutePath());
			
				if(targetDir.exists()) {
					
					System.out.println(" -> exist, checked");
					
					File[] kimGktgFiles = targetDir.listFiles(new FilenameFilter(){

						@Override
						public boolean accept(File dir, String name) {
							
							if(name.matches(kimGktgFilter)) {
								return true;
							} 
							
							return false;
						}
					});
					
					
					List<File> kimGktgFileList = new ArrayList<File>();
					
					for(File kimGktgFile : kimGktgFiles) {
						
						boolean isParsed = false;
						
						for(int j=0 ; j<parsedFileInfoList.size() ; j++) {
							
							Map<String, Object> parsedFileInfo = parsedFileInfoList.get(j);
							
							Date fileDt = sdf2.parse(parsedFileInfo.get("fileDt").toString());
							String fileName = parsedFileInfo.get("fileName").toString();
							
							if(kimGktgFile.getName().equals(fileName)) {
								isParsed = true;
							}
						}
						
						if(!isParsed) {
							kimGktgFileList.add(kimGktgFile);			
						}
					}
					
					kimGktgFileList.sort(new Comparator<File>() {

						@Override
						public int compare(File arg0, File arg1) {							
							return arg0.getName().compareTo(arg1.getName());
						}
					});
					
					this.drawGktgImages(kimGktgFileList, storePath);
					
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
	
	public void drawGktgImages(List<File> kimGktgFileList, String storePath) {
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHH");
		SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy/MM/dd/HH"); // 날짜 형식만 필요할 때 사용	
				
		System.out.println("MakeKimGktgImage [ Start Create Images ]");		
		
		for(int i=0 ; i<kimGktgFileList.size() ; i++) {
			
			File kimGktgFile = kimGktgFileList.get(i);
			
			System.out.println("Target File : " + kimGktgFile.getAbsolutePath());
			
			try {
				
				NetcdfDataset ncFile = NetcdfDataset.acquireDataset(kimGktgFile.getAbsolutePath(), null);
				String fileName = kimGktgFile.getName();
				
				Date fileDt = sdf2.parse(fileName.split("_")[5].split("\\.")[0]);
				
		        String savePath = storePath + "/KIM_GKTG_IMG/" + sdf3.format(fileDt) ;
		        
		        File saveDir = new File(savePath);
		        
		        if(!saveDir.exists()) {
		        	saveDir.mkdirs();
		        }		      
		        
		        this.generateImage(ncFile, fileName, savePath);		 
		        
				String query = this.insertFileProcInfo.replaceAll("\\{fileDt\\}", sdf2.format(fileDt))
						  							  .replaceAll("\\{fileName\\}", fileName)
						  							  .replaceAll("\\{filePath\\}", kimGktgFile.getAbsolutePath());
				
				this.dbManager.executeQuery(query);
				this.dbManager.commit();
				
			} catch (Exception e) {
				
			}
		}
		
		System.out.println("MakeKimGktgImage [ End Create Images ]");
	}
	
	public BoundLonLat generateImage(NetcdfDataset ncFile, String fileName, String savePath) {

		String varName = "GTGMAX";
		
		if(fileName.contains("_max_")) {
			varName = "GTGMAX";
		} else if(fileName.contains("_cat_")) {
			varName = "GTGDEF";
		} else if(fileName.contains("_mwt_")) {
			varName = "GTGMWT";
		} 
		
        BoundLonLat boundLonLat = null;
        BoundXY boundXY = null;
        Graphics2D ig2 = null;

        try {
            boundLonLat = this.modelGridUtil.getBoundLonLat();
            boundXY = this.modelGridUtil.getBoundXY();

            System.out.println(boundLonLat);

            KimGktgLegendFilter kimGktgLegendFilter = new KimGktgLegendFilter();

            int imgHeight = (int) Math.floor(
                (boundLonLat.getTop() - boundLonLat.getBottom())
                    * this.imageExpandFactor
                    * this.imageResizeFactor
            );

            int imgWidth = (int) Math.floor(
                (boundLonLat.getRight() - boundLonLat.getLeft())
                    * this.imageExpandFactor
                    * this.imageResizeFactor
            );

            System.out.println("\t-> " + boundLonLat + ", " + boundXY);

            int rows = this.modelGridUtil.getRows();
            int cols = this.modelGridUtil.getCols();

            double[] latInterval = GridCalcUtil.calculateCumulativeArr(this.modelGridUtil.getLatInterval());
            double[] lonInterval = GridCalcUtil.calculateCumulativeArr(this.modelGridUtil.getLonInterval());

            double[] mercatorRatio = GridCalcUtil.calculateCumulativeArr(
                GridCalcUtil.getLatitudeRatioList(
                    boundLonLat.getTop(),
                    boundLonLat.getBottom(),
                    imgHeight,
                    imgHeight
                )
            );

            System.out.println("\t-> Process Attribute [" + varName + "]");

            Variable var = ncFile.findVariable(varName);
            if (var == null) {
                throw new IllegalArgumentException("Variable not found: " + varName);
            }

            // 픽셀 경계 미리 계산
            int[] xPixel = new int[cols + 1];
            for (int l = 0; l <= cols; l++) {
                xPixel[l] = (int) Math.floor(
                    lonInterval[l] * this.imageExpandFactor * this.imageResizeFactor
                );
            }

            int[] yPixel = new int[rows + 1];
            for (int k = 0; k <= rows; k++) {
                double yCoord = boundLonLat.getTop() - latInterval[k];

                int idx = (int) Math.floor(
                    (yCoord - boundLonLat.getBottom())
                        * this.imageExpandFactor
                        * this.imageResizeFactor
                );

                if (idx < 0) {
                    idx = 0;
                } else if (idx >= mercatorRatio.length) {
                    idx = mercatorRatio.length - 1;
                }

                yPixel[k] = (int) Math.floor(mercatorRatio[idx]);
            }

            for(int j=0 ; j<=41 ; j++) {

                System.out.println("\t\t-> Start Read Variable [" + varName + "]");

                List<Range> rangeList = new ArrayList<Range>();
                rangeList.add(new Range(j, j));
                rangeList.add(new Range(
                    modelGridUtil.getModelHeight() - boundXY.getTop() - 1,
                    modelGridUtil.getModelHeight() - boundXY.getBottom() - 1
                ));
                rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));

                long t0 = System.currentTimeMillis();

                Float[][] values = GridCalcUtil.convertStorageToValues(
                    var.read(rangeList).getStorage(),
                    rows,
                    cols
                );

                long t1 = System.currentTimeMillis();

                System.out.println("\t\t-> End Read Variable [" + varName + "]");

                String imgFileName = fileName.replace(".nc", "") + "_" + String.format("%03d", j + 1) + ".png";
                
                File imageFile = new File(savePath + File.separator + imgFileName);

                BufferedImage bi = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
                ig2 = bi.createGraphics();

                System.out.println("\t\t-> Start Write Image [" + imageFile.getAbsolutePath() + "]");

                for (int k = 0; k < rows; k++) {

                    int yTop = yPixel[k];
                    int yBottom = yPixel[k + 1];
                    int rectY = Math.min(yTop, yBottom);
                    int rectHeight = Math.abs(yBottom - yTop);

                    if (rectHeight <= 0) {
                        continue;
                    }

                    int l = 0;
                    while (l < cols) {

                        Float valueObj = values[k][l];
                        if (valueObj == null) {
                            l++;
                            continue;
                        }

                        float v = valueObj.floatValue();
                        Color color = null;
                        
                		if(fileName.contains("_max_")) {
                			color =  kimGktgLegendFilter.getColor_GTGMAX(v);
                		} else if(fileName.contains("_cat_")) {
                			color =  kimGktgLegendFilter.getColor_GTGDEF(v);
                		} else if(fileName.contains("_mwt_")) {
                			color =  kimGktgLegendFilter.getColor_GTGMWT(v);
                		} 

                        if (color == null) {
                            l++;
                            continue;
                        }

                        int start = l;
                        l++;

                        while (l < cols) {
                            Float nextValueObj = values[k][l];
                            if (nextValueObj == null) {
                                break;
                            }

                            Color nextColor = kimGktgLegendFilter.getColor_GTGMAX(nextValueObj.floatValue());

                            if (nextColor == null || !nextColor.equals(color)) {
                                break;
                            }

                            l++;
                        }

                        int xLeft = xPixel[start];
                        int xRight = xPixel[l];
                        int rectX = Math.min(xLeft, xRight);
                        int rectWidth = Math.abs(xRight - xLeft);

                        if (rectWidth <= 0) {
                            continue;
                        }

                        ig2.setColor(color);
                        ig2.fillRect(rectX, rectY, rectWidth, rectHeight);
                    }
                }

                long t2 = System.currentTimeMillis();

                ig2.dispose();
                ig2 = null;

                if (this.imageResizeFactor > 1) {
                    bi = Thumbnails.of(bi)
                        .imageType(BufferedImage.TYPE_INT_ARGB)
                        .size(
                            imgWidth / this.imageResizeFactor,
                            imgHeight / this.imageResizeFactor
                        )
                        .asBufferedImage();
                }

                ImageIO.write(bi, "PNG", imageFile);

                long t3 = System.currentTimeMillis();

                System.out.println("\t\t-> End Write Image [" + imageFile.getAbsolutePath() + "]");
                System.out.println("\t\t-> Read Time   : " + (t1 - t0) + " ms");
                System.out.println("\t\t-> Render Time : " + (t2 - t1) + " ms");
                System.out.println("\t\t-> Write Time  : " + (t3 - t2) + " ms");
                System.out.println("\t\t-> Total Time  : " + (t3 - t0) + " ms");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (ig2 != null) {
                ig2.dispose();
            }
        }

        return boundLonLat;
    }
	
	public static void main(String[] args) {

		new MakeKimGktgImage().process();
	}
}
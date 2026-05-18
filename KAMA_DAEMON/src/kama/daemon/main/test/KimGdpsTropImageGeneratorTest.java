package kama.daemon.main.test;

import java.awt.BasicStroke;
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
import net.coobird.thumbnailator.resizers.configurations.Antialiasing;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

public class KimGdpsTropImageGeneratorTest {
	
	private ModelGridUtil modelGridUtil;
	
	private final int imageExpandFactor = 10;
	private final int imageResizeFactor = 1;
	
	private String varName = "Trop";
	
	public KimGdpsTropImageGeneratorTest() {
		
			
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
		
		double[] mapBound = new double[]{80, -80, 0, 360};
		
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
			
			System.out.println("\t-> Process Attribute ["+this.varName+"]");
			
			Variable tempVar = ncFile.findVariable("Temperature_isobaric");
			Variable hgtVar  = ncFile.findVariable("Geopotential_height_isobaric");

			/*
			 * pressure levels (Pa)
			 * index 0 = 50hPa
			 * index 23 = 1000hPa
			 */
			double[] pressureLevels = new double[]{
				5000.0, 7000.0, 10000.0, 15000.0,
				20000.0, 25000.0, 30000.0, 35000.0,
				40000.0, 45000.0, 50000.0, 55000.0,
				60000.0, 65000.0, 70000.0, 75000.0,
				80000.0, 85000.0, 87500.0, 90000.0,
				92500.0, 95000.0, 97500.0, 100000.0
			};

			long ms0 = System.currentTimeMillis();
			System.out.println("\t\t-> Start Read Temperature Variable");

			/*
			 * [level][row][col]
			 */
			float[][][] temperatureValues = new float[24][rows][cols];
			float[][][] heightValues = new float[24][rows][cols];

			/*
			 * temperature 전체 레벨 읽기
			 */
			for(int level=0 ; level<24 ; level++) {

			    List<Range> rangeList = new ArrayList<Range>();

			    rangeList.add(new Range(0, 0)); // time
			    rangeList.add(new Range(level, level));
			    rangeList.add(new Range(modelGridUtil.getModelHeight() - boundXY.getTop() - 1,
			                            modelGridUtil.getModelHeight() - boundXY.getBottom() - 1));
			    rangeList.add(new Range(boundXY.getLeft(), boundXY.getRight()));

			    float[][] tempValues =
			        GridCalcUtil.convertStorageToPrimitiveValuesReverse(
			            tempVar.read(rangeList).getStorage(), rows, cols
			        );

			    float[][] hgtValues =
			        GridCalcUtil.convertStorageToPrimitiveValuesReverse(
			            hgtVar.read(rangeList).getStorage(), rows, cols
			        );

			    temperatureValues[level] = tempValues;
			    heightValues[level] = hgtValues;

			    System.out.println("\t\t\t-> Read Level [" + pressureLevels[level] / 100.0 + " hPa]");
			}
			
			long ms1 = System.currentTimeMillis();

			System.out.println("\t\t-> End Read Temperature Variable");

			/*
			 * tropopause pressure result
			 */
			float[][] tropopausePressure = new float[rows][cols];
			float[][] tropopauseHeight = new float[rows][cols];

			System.out.println("\t\t-> Start Calculate Tropopause");

			/*
			 * grid loop
			 */
			double zMin = 3000.0;
			double zMax = 19000.0;
			double dz   = 250.0;
			
			int zCount = (int)Math.floor((zMax - zMin) / dz) + 1;

			double[] zProfile = new double[zCount];
			double[] tProfile = new double[zCount];
			double[] pProfile = new double[zCount];

			for(int k=0 ; k<rows ; k++) {

			    for(int l=0 ; l<cols ; l++) {

			        tropopausePressure[k][l] = Float.NaN;
			        tropopauseHeight[k][l] = Float.NaN;

			        int validCount = buildVerticalProfile(
			            temperatureValues,
			            heightValues,
			            pressureLevels,
			            k,
			            l,
			            zMin,
			            dz,
			            zCount,
			            zProfile,
			            tProfile,
			            pProfile
			        );

			        if(validCount < 3) {
			            continue;
			        }

			        Double previousLapse = null;
			        Double previousZMid = null;
			        Double previousPMid = null;

			        for(int i=1 ; i<validCount ; i++) {

			            double lapse =
			                -(tProfile[i] - tProfile[i - 1])
			                /
			                ((zProfile[i] - zProfile[i - 1]) / 1000.0);

			            double zMid = (zProfile[i] + zProfile[i - 1]) / 2.0;
			            double pMid = (pProfile[i] + pProfile[i - 1]) / 2.0;

			            if(previousLapse == null) {
			                previousLapse = lapse;
			                previousZMid = zMid;
			                previousPMid = pMid;
			                continue;
			            }

			            if(previousLapse > 2.0 &&
			               lapse <= 2.0 &&
			               checkWmoLayerProfile(tProfile, zProfile, i, validCount)) {

			                double interp;

			                if(Math.abs(previousLapse - lapse) < 0.0001) {
			                    interp = 0.0;
			                } else {
			                    interp =
			                        (previousLapse - 2.0)
			                        /
			                        (previousLapse - lapse);
			                }

			                interp = Math.max(0.0, Math.min(1.0, interp));

			                double tropZ =
			                    previousZMid + (zMid - previousZMid) * interp;

			                double tropP =
			                    previousPMid + (pMid - previousPMid) * interp;

			                tropopauseHeight[k][l] = (float)(tropZ / 1000.0);
			                tropopausePressure[k][l] = (float)(tropP / 100.0);

			                break;
			            }

			            previousLapse = lapse;
			            previousZMid = zMid;
			            previousPMid = pMid;
			        }
			    }
			}
			
			//fillMissingByNearestLevel(tropopauseHeight, tropopausePressure, heightValues, pressureLevels, rows, cols);
			
			tropopauseHeight = fillNaN(tropopauseHeight, rows, cols);
			tropopauseHeight = smoothField(tropopauseHeight, rows, cols);
			
			long ms2 = System.currentTimeMillis();

			System.out.println("\t\t-> End Calculate Tropopause");
			
			String imgFileName = fileName.replace(".gb2", "") + "_trop.png";
				
			File imageFile = new File(savePath + File.separator + imgFileName);
			
			BufferedImage bi = new BufferedImage(imgWidth, imgHeight, BufferedImage.TYPE_INT_ARGB);
			
			Graphics2D ig2 = bi.createGraphics();
			
			//ig2.setColor(new Color(245, 245, 245));
			//ig2.fillRect(0, 0, imgWidth, imgHeight);
			
			System.out.println("\t\t-> Start Write Image [" + imageFile.getAbsolutePath() + "]");
				
			for(int k=0 ; k<rows ; k++) {
				
				for(int l=0 ; l<cols ; l++) {
					
					float v = tropopauseHeight[k][l];
					
					Color c = kimGdpsLegendFilter.getColorTropopause(v);
					
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
			
			long ms3 = System.currentTimeMillis();
			
            ig2.dispose();
            ig2 = null;
			
			bi = Thumbnails.of(bi).imageType(BufferedImage.TYPE_INT_ARGB).size(imgWidth / this.imageResizeFactor, imgHeight / this.imageResizeFactor)
					.antialiasing(Antialiasing.ON).asBufferedImage();
			
			ImageIO.write(bi, "PNG", imageFile);
			System.out.println("\t\t-> End Write Image [" + imageFile.getAbsolutePath() + "]");

			long ms4 = System.currentTimeMillis();
		
            System.out.println("\t\t-> Read Time   : " + (ms1 - ms0) + " ms");
            System.out.println("\t\t-> Calculation Time : " + (ms2 - ms1) + " ms");
            System.out.println("\t\t-> Render Time : " + (ms3 - ms2) + " ms");
            System.out.println("\t\t-> Write Time : " + (ms4 - ms3) + " ms");
            System.out.println("\t\t-> Total Time : " + (ms4 - ms0) + " ms");
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return boundLonLat;
	}
	
	private boolean checkWmoLayerProfile(
	    double[] tProfile,
	    double[] zProfile,
	    int startIndex,
	    int validCount
	) {
	    double baseZ = zProfile[startIndex];
	    double topZ = baseZ + 2000.0;

	    double sum = 0.0;
	    int count = 0;

	    for(int i=startIndex + 1 ; i<validCount ; i++) {

	        if(zProfile[i] > topZ) {
	            break;
	        }

	        double lapse =
	            -(tProfile[i] - tProfile[i - 1])
	            /
	            ((zProfile[i] - zProfile[i - 1]) / 1000.0);

	        sum += lapse;
	        count++;
	    }

	    if(count == 0) {
	        return false;
	    }

	    return (sum / count) <= 2.0;
	}
	
	private int buildVerticalProfile(
	    float[][][] temperatureValues,
	    float[][][] heightValues,
	    double[] pressureLevels,
	    int k,
	    int l,
	    double zMin,
	    double dz,
	    int zCount,
	    double[] zProfile,
	    double[] tProfile,
	    double[] pProfile
	) {
	    int count = 0;

	    /*
	     * targetZ는 낮은 고도 -> 높은 고도 순서이므로
	     * pressure level도 아래층 -> 위층 방향으로 탐색해야 함
	     */
	    int level = pressureLevels.length - 2;

	    for(int i=0 ; i<zCount ; i++) {

	        double targetZ = zMin + dz * i;

	        while(level >= 0) {

	            double zUpper = heightValues[level][k][l];
	            double zLower = heightValues[level + 1][k][l];

	            if(Double.isNaN(zUpper) || Double.isNaN(zLower) ||
	               Math.abs(zUpper - zLower) < 0.001) {
	                level--;
	                continue;
	            }

	            double lo = Math.min(zUpper, zLower);
	            double hi = Math.max(zUpper, zLower);

	            /*
	             * 아직 현재 layer보다 아래면 다음 targetZ에서 다시 확인
	             */
	            if(targetZ < lo) {
	                break;
	            }

	            /*
	             * 현재 layer보다 위면 더 위 layer로 이동
	             */
	            if(targetZ > hi) {
	                level--;
	                continue;
	            }

	            double tUpper = temperatureValues[level][k][l];
	            double tLower = temperatureValues[level + 1][k][l];

	            if(Double.isNaN(tUpper) || Double.isNaN(tLower)) {
	                return count;
	            }

	            double ratio =
	                (targetZ - zLower)
	                /
	                (zUpper - zLower);

	            ratio = Math.max(0.0, Math.min(1.0, ratio));

	            zProfile[count] = targetZ;
	            tProfile[count] = tLower + (tUpper - tLower) * ratio;
	            pProfile[count] = pressureLevels[level + 1]
	                + (pressureLevels[level] - pressureLevels[level + 1]) * ratio;

	            count++;
	            break;
	        }
	    }

	    return count;
	}
	
	private float[][] fillNaN(float[][] src, int rows, int cols) {

	    float[][] dst = new float[rows][cols];

	    /*
	     * 원본 복사
	     */
	    for(int k=0 ; k<rows ; k++) {

	        for(int l=0 ; l<cols ; l++) {

	            dst[k][l] = src[k][l];
	        }
	    }

	    /*
	     * NaN fill
	     */
	    for(int k=1 ; k<rows-1 ; k++) {

	        for(int l=1 ; l<cols-1 ; l++) {

	            if(!Float.isNaN(src[k][l])) {
	                continue;
	            }

	            float sum = 0f;
	            int cnt = 0;

	            for(int dy=-1 ; dy<=1 ; dy++) {

	                for(int dx=-1 ; dx<=1 ; dx++) {

	                    if(dx == 0 && dy == 0) {
	                        continue;
	                    }

	                    float v = src[k + dy][l + dx];

	                    if(Float.isNaN(v)) {
	                        continue;
	                    }

	                    sum += v;
	                    cnt++;
	                }
	            }

	            /*
	             * 주변 유효값 3개 이상이면 평균 사용
	             */
	            if(cnt >= 3) {
	                dst[k][l] = sum / cnt;
	            }
	        }
	    }

	    return dst;
	}
	
	private float[][] smoothField(float[][] src, int rows, int cols) {

	    float[][] dst = new float[rows][cols];

	    /*
	     * 3x3 Gaussian kernel
	     *
	     * 1 2 1
	     * 2 4 2
	     * 1 2 1
	     */
	    int[][] kernel = new int[][]{
	        {1, 2, 1},
	        {2, 4, 2},
	        {1, 2, 1}
	    };

	    /*
	     * edge는 원본 유지
	     */
	    for(int k=0 ; k<rows ; k++) {

	        dst[k][0] = src[k][0];
	        dst[k][cols - 1] = src[k][cols - 1];
	    }

	    for(int l=0 ; l<cols ; l++) {

	        dst[0][l] = src[0][l];
	        dst[rows - 1][l] = src[rows - 1][l];
	    }

	    /*
	     * smoothing
	     */
	    for(int k=1 ; k<rows-1 ; k++) {

	        for(int l=1 ; l<cols-1 ; l++) {

	            /*
	             * NaN이면 그대로 유지
	             */
	            if(Float.isNaN(src[k][l])) {

	                dst[k][l] = Float.NaN;
	                continue;
	            }

	            float sum = 0f;
	            float weightSum = 0f;

	            for(int dy=-1 ; dy<=1 ; dy++) {

	                for(int dx=-1 ; dx<=1 ; dx++) {

	                    float v = src[k + dy][l + dx];

	                    /*
	                     * NaN 제외
	                     */
	                    if(Float.isNaN(v)) {
	                        continue;
	                    }

	                    float w = kernel[dy + 1][dx + 1];

	                    sum += v * w;
	                    weightSum += w;
	                }
	            }

	            if(weightSum > 0f) {
	                dst[k][l] = sum / weightSum;
	            } else {
	                dst[k][l] = src[k][l];
	            }
	        }
	    }

	    return dst;
	}
	
	private boolean interpolateAtHeight(
	    float[][][] temperatureValues,
	    float[][][] heightValues,
	    double[] pressureLevels,
	    int k,
	    int l,
	    double targetZ,
	    double[] out
	) {

	    for(int level=0 ; level<pressureLevels.length-1 ; level++) {

	        double zUpper = heightValues[level][k][l];
	        double zLower = heightValues[level + 1][k][l];

	        if(Double.isNaN(zUpper) || Double.isNaN(zLower)) {
	            continue;
	        }

	        double zMax = Math.max(zUpper, zLower);
	        double zMin = Math.min(zUpper, zLower);

	        if(targetZ < zMin || targetZ > zMax) {
	            continue;
	        }

	        double tUpper = temperatureValues[level][k][l];
	        double tLower = temperatureValues[level + 1][k][l];

	        double pUpper = pressureLevels[level];
	        double pLower = pressureLevels[level + 1];

	        if(Double.isNaN(tUpper) || Double.isNaN(tLower)) {
	            return false;
	        }

	        double ratio =
	            (targetZ - zLower)
	            /
	            (zUpper - zLower);

	        ratio = Math.max(0.0, Math.min(1.0, ratio));
	        
	        out[0] = tLower + (tUpper - tLower) * ratio;
	        out[1] = pLower + (pUpper - pLower) * ratio;

	        return true;
	    }

	    return false;
	}
	
	private void fillMissingByNearestLevel(
	    float[][] tropopauseHeight,
	    float[][] tropopausePressure,
	    float[][][] heightValues,
	    double[] pressureLevels,
	    int rows,
	    int cols
	) {
	    /*
	     * fallback level
	     * 14 = 700 hPa
	     * 13 = 650 hPa
	     * 12 = 600 hPa
	     */
	    int fallbackLevel = 18;

	    for(int k=0 ; k<rows ; k++) {

	        for(int l=0 ; l<cols ; l++) {

	            if(!Float.isNaN(tropopauseHeight[k][l])) {
	                continue;
	            }

	            float z = heightValues[fallbackLevel][k][l];

	            if(Float.isNaN(z)) {
	                continue;
	            }

	            tropopauseHeight[k][l] = z / 1000.0f;
	            tropopausePressure[k][l] = (float)(pressureLevels[fallbackLevel] / 100.0);
	        }
	    }
	}
	
	public static void main(String[] args) {
        
		KimGdpsTropImageGeneratorTest kimGdpsImageGeneratorTest = new KimGdpsTropImageGeneratorTest();
                  
		try {
						
			NetcdfDataset ncFile = NetcdfDataset.acquireDataset("F:\\data\\datastore\\KIM_GDPS\\2023\\07\\05\\00\\kim_g128_ne36_pres_h000.2023070500.gb2", null);
			String fileName = "kim_g128_ne36_pres_h000.2023070500.gb2";
	        String savePath = "F:/data/kim_gdps_trop_img";
	        
	        kimGdpsImageGeneratorTest.generateImages(ncFile, fileName, savePath);
	        
			
			
		} catch (Exception e) {
			
		}
		
		
	}
	
}

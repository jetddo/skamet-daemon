package kama.daemon.common.util.model;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 모델 격자 연산 유틸
 * @author Jetddo
 *
 */

public class ModelGridUtil {
	
	// 격자 종류
	public enum Model {
		GDPS,  // GDPS,
		LDPS,
		LDPS_REGRID,
		KTG,
		ICING_EA,
		ICING_KP,
		KMAPP_JEJU,
		KMAPP_GIMPO,
		KMAPP_INCHEON,
		KMAPP_MUAN,
		KMAPP_ULSAN,
		KMAPP_CHUNGJU,
		KMAPP_DAEGU,
		KMAPP_GYANGJU,
		KMAPP_KIMHAE,
		KMAPP_POHANG,
		KMAPP_SACHEON,
		KMAPP_YANGYANG,
		KMAPP_YEOSU,
		KIM_GDPS,
		KIM_RDPS,
		GKTG_ARCV,
		HOBS_RKSI,
		HOBS_RKPC,
		HOBS_RKNY,
		KIM_GKTG,
		KIM_KTG,
		KIM_KFIP
	}
	
	// 격자 기준 좌표 위치
	public enum Position {
		TOP_LEFT, 		// 제공되는 격자 기준점 좌표가 사각형의 왼쪽 위 
						// 격자의 맨 아랫쪽, 맨 우측의 격자 길이가 임의로 지정된다
		
		MIDDLE_CENTER 	// 제공되는 격자 기준점 좌표가 사각형의 정가운데
						// 사각형의 중점을 기준으로 인접한 사각형의 가로,세로 길이의 1/2를 더한 값으로 지정한다
						
	}
	
	private ByteOrder byteOrder = ByteOrder.BIG_ENDIAN;

	// 격자 기준 좌표 위치
	private Position position;
	
	// 모델 격자 갯수
	private int modelWidth = 0;
	private int modelHeight = 0;
	
	private int numberFix = 3;
	
	// 모델 격자 파일 버퍼
	private FloatBuffer latBuffer = null;
	private FloatBuffer lonBuffer = null;
	
	// 거리 최소값
	private double minDistance = Double.MAX_VALUE;
	
	// 격자 최소 좌표
	private int minX = 0;	
	private int minY = 0;
	
	private double maxTop = Double.MIN_VALUE;
	private double minBottom = Double.MAX_VALUE;
	private double minLeft = Double.MAX_VALUE;
	private double maxRight = Double.MIN_VALUE;
	
	// 격자 범위 좌표
	private BoundXY boundXY = null;
	
	// 격자 범위 위경도
	private BoundLonLat boundLonLat = null;

	// 기본 위도, 경도 격자 간격
	private double defaultLatInterval;
	private double defaultLonInterval;
	
	// 격자 간격 
	private double[] latInterval;
	private double[] lonInterval;
	
	// 밀려있는 경도
	private int lonShift = 0;
	
	// 폴리곤 정보
	private List<Map<String, Object>> polygonDataList = new ArrayList<Map<String, Object>>();
	
	public ModelGridUtil() {		
		
	}
	
	public ModelGridUtil(Model model, String latPath, String lonPath) {		
		this(model, Position.TOP_LEFT, latPath, lonPath);
	}
	
	public int getModelWidth() {
		return modelWidth;
	}

	public void setModelWidth(int modelWidth) {
		this.modelWidth = modelWidth;
	}

	public int getModelHeight() {
		return modelHeight;
	}

	public void setModelHeight(int modelHeight) {
		this.modelHeight = modelHeight;
	}
	
	public void setNumberFix(int numberFix) {
		this.numberFix = numberFix;
	}
	
	public ModelGridUtil(Model model, Position position, String latPath, String lonPath) {
		
		this.position = position;
		
		if(Model.GDPS.equals(model)) {
			
			this.modelWidth = 2560;
			this.modelHeight = 1920;
			
			this.defaultLatInterval = 0.09375;
			this.defaultLonInterval = 0.140625;
			
			this.byteOrder = ByteOrder.BIG_ENDIAN;
			
		} else if(Model.LDPS.equals(model)) {
			
			this.modelWidth = 1188;
			this.modelHeight = 1148;
	
			this.byteOrder = ByteOrder.LITTLE_ENDIAN;
			
		} else if(Model.LDPS_REGRID.equals(model)) {
			
			this.modelWidth = 1188;
			this.modelHeight = 1148;
			
			this.defaultLatInterval = 0.020730990581961435;
			this.defaultLonInterval = 0.03481551751230781;
	
			this.byteOrder = ByteOrder.BIG_ENDIAN;
			
		} else if(Model.ICING_EA.equals(model)) {
			
			this.modelWidth = 490;
			this.modelHeight = 418;
				
			this.byteOrder = ByteOrder.BIG_ENDIAN;
			
		} else if(Model.KTG.equals(model)) {
			
			this.modelWidth = 490;
			this.modelHeight = 418;
				
			this.byteOrder = ByteOrder.LITTLE_ENDIAN;
			
		} else if(Model.ICING_KP.equals(model)) {
			
			this.modelWidth = 601;
			this.modelHeight = 780;
	
			this.byteOrder = ByteOrder.BIG_ENDIAN;
			
		} else if(Model.KMAPP_JEJU.equals(model)) {
			
			this.modelWidth = 174;
			this.modelHeight = 147;
	
			this.byteOrder = ByteOrder.BIG_ENDIAN;
			
		} else if(Model.KMAPP_GIMPO.equals(model)) {
			
			this.modelWidth = 200;
			this.modelHeight = 202;
	
			this.byteOrder = ByteOrder.BIG_ENDIAN;
			
		} else if(Model.KMAPP_INCHEON.equals(model)) {
			
			this.modelWidth = 199;
			this.modelHeight = 203;
	
			this.byteOrder = ByteOrder.BIG_ENDIAN;
			
		} else if(Model.KMAPP_MUAN.equals(model)) {
			
			this.modelWidth = 199;
			this.modelHeight = 202;
	
			this.byteOrder = ByteOrder.BIG_ENDIAN;
			
		} else if(Model.KMAPP_ULSAN.equals(model)) {
			
			this.modelWidth = 206;
			this.modelHeight = 196;
	
			this.byteOrder = ByteOrder.BIG_ENDIAN;
				
		} else if(Model.KMAPP_CHUNGJU.equals(model)) {
			
			this.modelWidth = 202;
			this.modelHeight = 200;
	
			this.byteOrder = ByteOrder.BIG_ENDIAN;
			
		} else if(Model.KMAPP_DAEGU.equals(model)) {
			
			this.modelWidth = 203;
			this.modelHeight = 197;
	
			this.byteOrder = ByteOrder.BIG_ENDIAN;
			
		} else if(Model.KMAPP_GYANGJU.equals(model)) {
			
			this.modelWidth = 202;
			this.modelHeight = 203;
	
			this.byteOrder = ByteOrder.BIG_ENDIAN;
			
		} else if(Model.KMAPP_KIMHAE.equals(model)) {
			
			this.modelWidth = 207;
			this.modelHeight = 195;
	
			this.byteOrder = ByteOrder.BIG_ENDIAN;
			
		} else if(Model.KMAPP_POHANG.equals(model)) {
			
			this.modelWidth = 202;
			this.modelHeight = 195;
	
			this.byteOrder = ByteOrder.BIG_ENDIAN;
			
		} else if(Model.KMAPP_SACHEON.equals(model)) {
			
			this.modelWidth = 206;
			this.modelHeight = 202;
	
			this.byteOrder = ByteOrder.BIG_ENDIAN;
			
		} else if(Model.KMAPP_YANGYANG.equals(model)) {
			
			this.modelWidth = 211;
			this.modelHeight = 196;
	
			this.byteOrder = ByteOrder.BIG_ENDIAN;
			
		} else if(Model.KMAPP_YEOSU.equals(model)) {
			
			this.modelWidth = 204;
			this.modelHeight = 195;
	
			this.byteOrder = ByteOrder.BIG_ENDIAN;
			
		} else if(Model.KIM_GDPS.equals(model)) {
			
			this.modelWidth = 2880;
			this.modelHeight = 1440;
			
			this.defaultLatInterval = 0.125;
			this.defaultLonInterval = 0.125;
	
			this.byteOrder = ByteOrder.BIG_ENDIAN;
			
		} else if(Model.KIM_RDPS.equals(model)) {
			
			this.modelWidth = 1049;
			this.modelHeight = 839;
	
			this.byteOrder = ByteOrder.BIG_ENDIAN;
			
		} else if(Model.GKTG_ARCV.equals(model)) {
			
			this.modelWidth = 214;
			this.modelHeight = 214;
			
			this.defaultLatInterval = 0.093796;
			this.defaultLonInterval = 0.140625;
	
			this.byteOrder = ByteOrder.BIG_ENDIAN;
			
		} else if (Model.HOBS_RKSI.equals(model)) {

			this.modelWidth = 321;
			this.modelHeight = 321;

			this.byteOrder = ByteOrder.BIG_ENDIAN;

		} else if (Model.HOBS_RKPC.equals(model)) {

			this.modelWidth = 321;
			this.modelHeight = 321;

			this.byteOrder = ByteOrder.BIG_ENDIAN;

		} else if (Model.HOBS_RKNY.equals(model)) {

			this.modelWidth = 321;
			this.modelHeight = 321;

			this.byteOrder = ByteOrder.BIG_ENDIAN;

		} else if (Model.KIM_GKTG.equals(model)) {

			this.modelWidth = 4320;
			this.modelHeight = 2160;
			
			this.defaultLatInterval = 1d/12d;
			this.defaultLonInterval = 1d/12d;

			this.byteOrder = ByteOrder.BIG_ENDIAN;

		} else if(Model.KIM_KTG.equals(model)) {
			
			this.modelWidth = 735;
			this.modelHeight = 627;
				
			this.byteOrder = ByteOrder.BIG_ENDIAN;
			
		} else if(Model.KIM_KFIP.equals(model)) {
			
			this.modelWidth = 673;
			this.modelHeight = 444;
				
			this.byteOrder = ByteOrder.BIG_ENDIAN;
			
			this.defaultLatInterval = 1d/12d;
			this.defaultLonInterval = 1d/12d;
			
		} else {
			throw new IllegalArgumentException("지원하지 않는 모델입니다.");
		}
		
		this.latBuffer = this.getGridBuffer(latPath);
		this.lonBuffer = this.getGridBuffer(lonPath);
	}
	
	public ModelGridUtil(Model model, Position position, String latPath, String lonPath, int lonShift) {
				
		this(model, position, latPath, lonPath);
		
		this.lonShift = lonShift;
	}
		
	private FloatBuffer getGridBuffer(String fileName) {
				
		RandomAccessFile raf = null;
		FileChannel channel = null;
		FloatBuffer floatBuffer = null;
		
		try {
		
			raf = new RandomAccessFile(fileName, "r");
			
			channel = raf.getChannel();
			
			ByteBuffer byteBuffer = ByteBuffer.allocateDirect((int)raf.length()).order(this.byteOrder);
			channel.read(byteBuffer);
			byteBuffer.clear();
			
			floatBuffer = byteBuffer.asFloatBuffer();
		
		} catch (IOException ioe) {
			
		} finally {
			
			try {
			
				channel.close();
				raf.close();
			
			} catch (IOException ioe) { 
				
			}
		}
		
		return floatBuffer;
	}

	/**
	 * 지점사이 거리 구하기 위경도
	 * @param lon
	 * @param lat
	 * @param lonLat
	 * @return
	 */
	private double getDistance(double lon, double lat, PointLonLat lonLat) {
		
    	double dx = Math.abs(lon - lonLat.getLon());
    	double dy = Math.abs(lat - lonLat.getLat());
    	
    	return dx + dy;
    }
    
	/**
	 * 지점사이 거리 구하기 좌표
	 * @param x
	 * @param y
	 * @param lonLat
	 * @return
	 */
	private double getDistance(int x, int y, PointLonLat lonLat) {
		
    	int pos = y * this.modelWidth + x;
    	
    	try {
    	
    		this.getDistance(this.lonBuffer.get(pos), this.latBuffer.get(pos), lonLat);
    	
    	} catch(Exception e) {}
    	
    	return this.getDistance(this.lonBuffer.get(pos), this.latBuffer.get(pos), lonLat);
    }
	
	public PointLonLat getPointLonLat(int x, int y) {
		
		int pos = y * this.modelWidth + x;
		
		return new PointLonLat(this.lonBuffer.get(pos), this.latBuffer.get(pos));
	}
	
	/**
	 * 최소지점 지정
	 * @param x
	 * @param y
	 * @param distance
	 */
	private void setClosePointInfo(int x, int y, double distance) {
		
		if(distance < this.minDistance) {
			
			this.minX = x;
			this.minY = y;
			this.minDistance = distance;	
		}		
	}
	
	/**
	 * lonLat 과 가장 가까운 격자 찾기
	 * @param x
	 * @param y
	 * @param lonLat
	 * @return
	 */
	public PointXY searchGrid(int x, int y, PointLonLat lonLat) {
			
		// x, y 를 가장 가까운 격자로 지정
		this.setClosePointInfo(x, y, this.getDistance(x, y, lonLat));
		 
		if(x > 0) {
			
			this.setClosePointInfo(x-1, y, this.getDistance(x-1, y, lonLat));
			
			if(y > 0) {				
				this.setClosePointInfo(x-1, y-1, this.getDistance(x-1, y-1, lonLat));
				this.setClosePointInfo(x, y-1, this.getDistance(x, y-1, lonLat));
			}
			
			if(y < this.modelHeight - 1) {
				this.setClosePointInfo(x-1, y+1, this.getDistance(x-1, y+1, lonLat));
				this.setClosePointInfo(x, y+1, this.getDistance(x, y+1, lonLat));
			}
		}
		
		if(x < this.modelWidth - 1) {
			
			this.setClosePointInfo(x+1, y, this.getDistance(x+1, y, lonLat));
			
			if(y > 0) {				
				this.setClosePointInfo(x+1, y-1, this.getDistance(x+1, y-1, lonLat));
				this.setClosePointInfo(x, y-1, this.getDistance(x, y-1, lonLat));
			}
			
			if(y < this.modelHeight - 1) {
				this.setClosePointInfo(x+1, y+1, this.getDistance(x+1, y+1, lonLat));
				this.setClosePointInfo(x, y+1, this.getDistance(x, y+1, lonLat));
			}			
		}		
		
		if(x == this.minX && y == this.minY) {
			return new PointXY(-1, -1);
		}
		
		return new PointXY(this.minX, this.minY);
	}
	
	/**
	 * 가까운 격자 좌표 구하기
	 * @param lon
	 * @param lat
	 * @return
	 */
	public PointXY getPointXY(double lon, double lat) {
		
		this.minDistance = Double.MAX_VALUE;
		this.minX = 0;
		this.minY = 0;
		
		PointLonLat lonLat = new PointLonLat(lon + this.lonShift, lat);
		
		PointXY xy = new PointXY(0, 0);
		
		while(true) {
			
			xy = this.searchGrid(xy.getX(), xy.getY(), lonLat);
			
			if(xy.getX() < 0 && xy.getY() < 0) {
				break;
			}
		}
		
		return new PointXY(this.minX, this.minY);
	}
	
	public Map<String, Object> getDistanceGridInfo(PointXY topLeftXY) {
		
		PointLonLat topLeftLonLat = this.getPointLonLat(topLeftXY.getX(), topLeftXY.getY());
		PointLonLat topRightLonLat = null;
		PointLonLat bottomLeftLonLat = null;
		PointLonLat bottomRightLonLat = null;
		
		if(topLeftXY.getX() == this.modelWidth-1) {
			
			PointLonLat leftTopLeftLonLat = this.getPointLonLat(topLeftXY.getX()-1, topLeftXY.getY());
			
			topRightLonLat = new PointLonLat(
					topLeftLonLat.getLon() + (topLeftLonLat.getLon() - leftTopLeftLonLat.getLon()), 
					topLeftLonLat.getLat() + (topLeftLonLat.getLat() - leftTopLeftLonLat.getLat()));
			
			if(topLeftXY.getY() == 0) {
				
				PointLonLat _topLeftLonLat = this.getPointLonLat(topLeftXY.getX()-1, topLeftXY.getY());
				PointLonLat _topRightLonLat = this.getPointLonLat(topLeftXY.getX(), topLeftXY.getY());
				PointLonLat _upTopLeftLonLat = this.getPointLonLat(topLeftXY.getX()-1, topLeftXY.getY()+1);
				PointLonLat _upTopRightLonLat = this.getPointLonLat(topLeftXY.getX(), topLeftXY.getY()+1);
								
				PointLonLat _bottomLeftLonLat = new PointLonLat(
						_topLeftLonLat.getLon() + (_topLeftLonLat.getLon() - _upTopLeftLonLat.getLon()), 
						_topLeftLonLat.getLat() + (_topLeftLonLat.getLat() - _upTopLeftLonLat.getLat()));
				
				PointLonLat _bottomRightLonLat = new PointLonLat(
						_topRightLonLat.getLon() + (_topRightLonLat.getLon() - _upTopRightLonLat.getLon()), 
						_topRightLonLat.getLat() + (_topRightLonLat.getLat() - _upTopRightLonLat.getLat())); 
				
				bottomLeftLonLat = _bottomRightLonLat;
				
				bottomRightLonLat = new PointLonLat(
						_bottomRightLonLat.getLon() + (_bottomRightLonLat.getLon() - _bottomLeftLonLat.getLon()), 
						_bottomRightLonLat.getLat() + (_bottomRightLonLat.getLat() - _bottomLeftLonLat.getLat()));
				
			} else {
				
				PointLonLat leftBottomLeftLonLat = this.getPointLonLat(topLeftXY.getX()-1, topLeftXY.getY()-1);
				
				bottomLeftLonLat = new PointLonLat(
						this.getPointLonLat(topLeftXY.getX(), topLeftXY.getY()-1).getLon(), 
						this.getPointLonLat(topLeftXY.getX(), topLeftXY.getY()-1).getLat());
				
				bottomRightLonLat = new PointLonLat(
						bottomLeftLonLat.getLon() + (bottomLeftLonLat.getLon() - leftBottomLeftLonLat.getLon()), 
						bottomLeftLonLat.getLat() + (bottomLeftLonLat.getLat() - leftBottomLeftLonLat.getLat()));
			}
			
		} else {
			
			topRightLonLat = new PointLonLat(
					this.getPointLonLat(topLeftXY.getX()+1, topLeftXY.getY()).getLon(), 
					this.getPointLonLat(topLeftXY.getX()+1, topLeftXY.getY()).getLat());
			
			if(topLeftXY.getY() == 0) {
				
				PointLonLat upTopLeftLonLat = this.getPointLonLat(topLeftXY.getX(), topLeftXY.getY()+1);
				PointLonLat upTopRightLonLat = this.getPointLonLat(topLeftXY.getX()+1, topLeftXY.getY()+1);
				
				bottomLeftLonLat = new PointLonLat(
						topLeftLonLat.getLon() + (topLeftLonLat.getLon() - upTopLeftLonLat.getLon()), 
						topLeftLonLat.getLat() + (topLeftLonLat.getLat() - upTopLeftLonLat.getLat()));
				
				bottomRightLonLat = new PointLonLat(
						topRightLonLat.getLon() + (topRightLonLat.getLon() - upTopRightLonLat.getLon()), 
						topRightLonLat.getLat() + (topRightLonLat.getLat() - upTopRightLonLat.getLat())); 
				
			} else {
				
				bottomLeftLonLat = new PointLonLat(
						this.getPointLonLat(topLeftXY.getX(), topLeftXY.getY()-1).getLon(), 
						this.getPointLonLat(topLeftXY.getX(), topLeftXY.getY()-1).getLat());
				
				bottomRightLonLat = new PointLonLat(
						this.getPointLonLat(topLeftXY.getX()+1, topLeftXY.getY()-1).getLon(), 
						this.getPointLonLat(topLeftXY.getX()+1, topLeftXY.getY()-1).getLat());
			}
		}
		
		this.maxTop = Math.max(this.maxTop, topRightLonLat.getLat());
		this.maxTop = Math.max(topLeftLonLat.getLat(), this.maxTop);
		this.minBottom = Math.min(this.minBottom, bottomRightLonLat.getLat());
		this.minBottom = Math.min(bottomLeftLonLat.getLat(), this.minBottom);
		this.minLeft = Math.min(this.minLeft, bottomLeftLonLat.getLon());
		this.minLeft = Math.min(topLeftLonLat.getLon(), this.minLeft);
		this.maxRight = Math.max(this.maxRight, bottomRightLonLat.getLon());
		this.maxRight = Math.max(topRightLonLat.getLon(), this.maxRight);
		
		Map<String, Object> polygonData = new HashMap<String, Object>();
	
		polygonData.put("coordinates", new double[][]{
			{this.NumberToFixed(topLeftLonLat.getLon()), this.NumberToFixed(topLeftLonLat.getLat())},
			{this.NumberToFixed(topRightLonLat.getLon()), this.NumberToFixed(topRightLonLat.getLat())},
			{this.NumberToFixed(bottomRightLonLat.getLon()), this.NumberToFixed(bottomRightLonLat.getLat())},
			{this.NumberToFixed(bottomLeftLonLat.getLon()), this.NumberToFixed(bottomLeftLonLat.getLat())},
			{this.NumberToFixed(topLeftLonLat.getLon()), this.NumberToFixed(topLeftLonLat.getLat())}
		});
		
		polygonData.put("x", topLeftXY.getX());
		polygonData.put("y", topLeftXY.getY());
		
		return polygonData;
	}
	
	public void setMultipleGridBoundInfoforDistanceGrid(double[] mapBound) {
		this.setMultipleGridBoundInfoforDistanceGrid(mapBound[0], mapBound[1], mapBound[2], mapBound[3]);
	}
	
	public void setMultipleGridBoundInfoforDistanceGrid(double mapTop, double mapBottom, double mapLeft, double mapRight) {
		
		this.maxTop = Double.MIN_VALUE;
		this.minBottom = Double.MAX_VALUE;
		this.minLeft = Double.MAX_VALUE;
		this.maxRight = Double.MIN_VALUE;
		
		this.polygonDataList.clear();
		
		PointXY xyLeftTop = this.getPointXY(mapLeft, mapTop);		
		PointXY xyRightTop = this.getPointXY(mapRight, mapTop);
		PointXY xyRightBottom = this.getPointXY(mapRight, mapBottom);
		PointXY xyLeftBottom = this.getPointXY(mapLeft, mapBottom);
		
		int gridTop = Math.max(xyLeftTop.getY(), xyRightTop.getY());
		int gridBottom = Math.min(xyLeftBottom.getY(), xyRightBottom.getY());
		int gridLeft = Math.min(xyLeftTop.getX(), xyLeftBottom.getX());
		int gridRight = Math.max(xyRightTop.getX(), xyRightBottom.getX());
		
		if(gridTop < this.modelHeight-1) {
			gridTop++;
		}
		
		if(gridBottom > 0) {
			gridBottom = gridBottom > 20 ? gridBottom - 20 : 0;
		}
		
		if(gridLeft > 0) {
			gridLeft--;
		}
		
		if(gridRight < this.modelWidth-1) {
			gridRight++;
		}
		
		
		for(int i=gridBottom ; i<=gridTop ; i++) {			
			for(int j=gridLeft ; j<=gridRight ; j++) {
				
				Map<String, Object> gridInfo = this.getDistanceGridInfo(new PointXY(j,i));
				
				this.polygonDataList.add(gridInfo);				
			}
		}
		
		this.boundXY = new BoundXY(gridTop, gridBottom, gridLeft, gridRight);
		this.boundLonLat = new BoundLonLat(this.maxTop, this.minBottom, this.minLeft, this.maxRight);
	}
	
	public void setSingleGridBoundInfoforDistanceGrid(double lon, double lat) {
		
		this.maxTop = Double.MIN_VALUE;
		this.minBottom = Double.MAX_VALUE;
		this.minLeft = Double.MAX_VALUE;
		this.maxRight = Double.MIN_VALUE;
		
		this.polygonDataList.clear();
		
		PointXY pointXY = this.getPointXY(lon, lat);
		PointLonLat pointLonLat = this.getPointLonLat(pointXY.getX(), pointXY.getY());
		
		double sLat = pointLonLat.getLat();
		double sLon = pointLonLat.getLon();
		
		int pointX = pointXY.getX();
		int pointY = pointXY.getY();
		
		if(sLat > lat) {
			pointY = pointXY.getY();
		} else {
			if(pointXY.getY() < this.modelHeight - 1) {
				pointY = pointXY.getY() + 1;	
			}	
		}
		
		if(sLon > lon && pointXY.getX() > 0) {				
			pointX = pointXY.getX() - 1;
		} else {				
			pointX = pointXY.getX();
		}
		
		Map<String, Object> gridInfo = this.getDistanceGridInfo(new PointXY(pointX, pointY));
		
		this.polygonDataList.add(gridInfo);	
		
		this.boundXY = new BoundXY(pointY, pointY, pointX, pointX);
		this.boundLonLat = new BoundLonLat(this.maxTop, this.minBottom, this.minLeft, this.maxRight);
	}
	
	/**
	 * 해당하는 격자 top, bottom, left, right 구하기
	 * @param lon
	 * @param lat
	 */
	public void setSingleGridBoundInfoforLatLonGrid(double lon, double lat) {
		
		PointXY pointXY = this.getPointXY(lon, lat);
		PointLonLat pointLonLat = this.getPointLonLat(pointXY.getX(), pointXY.getY());
		
		double sLat = pointLonLat.getLat();
		double sLon = pointLonLat.getLon();
		
		int pointX = pointXY.getX();
		int pointY = pointXY.getY();
		
		if(this.position == Position.TOP_LEFT) {
			
			double lonWidth = 0d;
			double latWidth = 0d;
			
			if(sLat > lat) {
				pointY = pointXY.getY();
			} else {
				if(pointXY.getY() < this.modelHeight - 1) {
					pointY = pointXY.getY() + 1;	
				}	
			}
			
			if(sLon > lon && pointXY.getX() > 0) {				
				pointX = pointXY.getX() - 1;
			} else {				
				pointX = pointXY.getX();
			}
			
			if(pointY > 0) {
				latWidth = this.getPointLonLat(0, pointY).getLat() - this.getPointLonLat(0, pointY-1).getLat();
			} else {
				latWidth = this.defaultLatInterval;
			}
			
			if(pointX < this.modelWidth - 1) {
				lonWidth = this.getPointLonLat(pointX+1, 0).getLon() - this.getPointLonLat(pointX, 0).getLon();
			} else {
				lonWidth = this.defaultLonInterval;
			}
		
			this.boundXY = new BoundXY(pointY, pointY, pointX, pointX);
			
			PointLonLat lonLatLeftTop = this.getPointLonLat(pointX, pointY);
			
			this.boundLonLat = new BoundLonLat(
					lonLatLeftTop.getLat(), 
					lonLatLeftTop.getLat() - latWidth, 
					lonLatLeftTop.getLon() - this.lonShift, 
					lonLatLeftTop.getLon() + lonWidth - this.lonShift);
				
		} else if(this.position == Position.MIDDLE_CENTER) {
			
			double topLatWidth = 0d;
			double leftLonWidth = 0d;
			double bottomLatWidth = 0d;
			double rightLonWidth = 0d;
			
			double compareLatInterval = 0d;
			double compareLonInterval = 0d;
			
			if(pointXY.getY() < this.modelHeight - 1) {
				compareLatInterval = (this.getPointLonLat(0, pointXY.getY() + 1).getLat() - this.getPointLonLat(0, pointXY.getY()).getLat()) / 2;
			} else {
				compareLatInterval = this.defaultLatInterval / 2;
			}
					
			if(pointXY.getX() > 0) {
				compareLonInterval = (this.getPointLonLat(pointXY.getX(), 0).getLon() - this.getPointLonLat(pointXY.getX() - 1, 0).getLon()) / 2;
			} else {
				compareLonInterval = this.defaultLonInterval / 2;
			}
			
			if(sLat + compareLatInterval > lat) {
				pointY = pointXY.getY();
			} else {
				
				if(pointXY.getY() < this.modelHeight - 1) {
					pointY = pointXY.getY() + 1;	
				}	
			}
			
			if(sLon - compareLonInterval > lon && pointXY.getX() > 0) {				
				pointX = pointXY.getX() - 1;
			} else {				
				pointX = pointXY.getX();
			}
				
			if(pointY < this.modelHeight - 1) {
				topLatWidth = (this.getPointLonLat(0, pointY + 1).getLat() - this.getPointLonLat(0, pointY).getLat()) / 2;
			} else {
				topLatWidth = this.defaultLatInterval / 2;
			}
			
			if(pointX > 0) {
				leftLonWidth = (this.getPointLonLat(pointX, 0).getLon() - this.getPointLonLat(pointX - 1, 0).getLon()) / 2;
			} else {
				leftLonWidth = this.defaultLonInterval / 2;
			}
			
			if(pointY > 0) {
				bottomLatWidth = (this.getPointLonLat(0, pointY).getLat() - this.getPointLonLat(0, pointY - 1).getLat()) / 2;
			} else {
				bottomLatWidth = this.defaultLatInterval / 2;
			}
			
			if(pointX < this.modelWidth - 1) {
				rightLonWidth = (this.getPointLonLat(pointX + 1, 0).getLon() - this.getPointLonLat(pointX, 0).getLon()) / 2;
			} else {
				rightLonWidth = this.defaultLonInterval / 2;
			}
					
			this.boundXY = new BoundXY(pointY, pointY, pointX, pointX);
			
			PointLonLat lonLatLeftTop = this.getPointLonLat(pointX, pointY);
			
			this.boundLonLat = new BoundLonLat(
					lonLatLeftTop.getLat() + topLatWidth, 
					lonLatLeftTop.getLat() - bottomLatWidth, 
					lonLatLeftTop.getLon() - leftLonWidth - this.lonShift, 
					lonLatLeftTop.getLon() + rightLonWidth - this.lonShift);
		}
	}
	
	public void setMultipleGridBoundInfoforLatLonGrid(double[] mapBound) {
		this.setMultipleGridBoundInfoforLatLonGrid(mapBound[0], mapBound[1], mapBound[2], mapBound[3]);
	}
	
	/**
	 * 해당하는 범위의 top, bottom, left, right 구하기
	 * @param mapTop
	 * @param mapBottom
	 * @param mapLeft
	 * @param mapRight
	 */
	public void setMultipleGridBoundInfoforLatLonGrid(double mapTop, double mapBottom, double mapLeft, double mapRight) {
		
		PointXY xyLeftTop = this.getPointXY(mapLeft, mapTop);		
		PointXY xyRightTop = this.getPointXY(mapRight, mapTop);
		PointXY xyRightBottom = this.getPointXY(mapRight, mapBottom);
		PointXY xyLeftBottom = this.getPointXY(mapLeft, mapBottom);
		
		int gridTop = Math.max(xyLeftTop.getY(), xyRightTop.getY());
		int gridBottom = Math.min(xyLeftBottom.getY(), xyRightBottom.getY());
		int gridLeft = Math.min(xyLeftTop.getX(), xyLeftBottom.getX());
		int gridRight = Math.max(xyRightTop.getX(), xyRightBottom.getX());
			
		if(gridTop < this.modelHeight-1) {
			gridTop++;
		}
		
		if(gridBottom > 0) {
			gridBottom--;
		}
		
		if(gridLeft > 0) {
			gridLeft--;
		}
		
		if(gridRight < this.modelWidth-1) {
			gridRight++;
		}
		
		PointLonLat lonLatLeftTop = this.getPointLonLat(gridLeft, gridTop);
		PointLonLat lonLatRightBottom = this.getPointLonLat(gridRight, gridBottom);
		
		this.boundXY = new BoundXY(gridTop, gridBottom, gridLeft, gridRight);
		
		if(this.position == Position.TOP_LEFT) {
			
			double lastLatInterval = 0d;
			double lastLonInterval = 0d;
				
			if(gridBottom > 0) {
				lastLatInterval = lonLatRightBottom.getLat() - this.getPointLonLat(0, gridBottom - 1).getLat();
			} else {
				lastLatInterval = this.defaultLatInterval;
			}
			
			if(gridRight < this.modelWidth - 1) {
				lastLonInterval = this.getPointLonLat(gridRight + 1, 0).getLon() - lonLatRightBottom.getLon();
			} else {
				lastLonInterval = this.defaultLonInterval;
			}
			
			this.boundLonLat = new BoundLonLat(
					lonLatLeftTop.getLat(), 
					lonLatRightBottom.getLat() - lastLatInterval, 
					lonLatLeftTop.getLon() - this.lonShift, 
					lonLatRightBottom.getLon() + lastLonInterval - this.lonShift);
							
			this.latInterval = new double[this.getRows()];
			
			for(int i=gridTop-1 ; i>=gridBottom ; i--) {				
				this.latInterval[gridTop - i - 1] = this.getPointLonLat(0, i+1).getLat() - this.getPointLonLat(0, i).getLat();
			}
			
			this.latInterval[this.getRows()-1] = lastLatInterval;
						
			this.lonInterval = new double[this.getCols()];
			
			for(int i=gridLeft+1 ; i<=gridRight ; i++) {			
				this.lonInterval[i - gridLeft - 1] = this.getPointLonLat(i, 0).getLon() - this.getPointLonLat(i-1, 0).getLon();
			}
			
			this.lonInterval[this.getCols()-1] = lastLonInterval;
			
		} else if(this.position == Position.MIDDLE_CENTER) {
			
			double firstLatInterval = 0d;
			double firstLonInterval = 0d;
			double lastLatInterval = 0d;
			double lastLonInterval = 0d;
			
			if(gridTop < this.modelHeight - 1) {
				firstLatInterval = (this.getPointLonLat(0, gridTop + 1).getLat() - lonLatLeftTop.getLat()) / 2;
			} else {
				firstLatInterval = this.defaultLatInterval / 2;
			}
			
			if(gridLeft > 0) {
				firstLonInterval = (lonLatLeftTop.getLon() - this.getPointLonLat(gridLeft - 1, 0).getLon()) / 2;
			} else {
				firstLonInterval = this.defaultLonInterval / 2;
			}
			
			if(gridBottom > 0) {
				lastLatInterval = (lonLatRightBottom.getLat() - this.getPointLonLat(0, gridBottom - 1).getLat()) / 2;
			} else {
				lastLatInterval = this.defaultLatInterval / 2;
			}
			
			if(gridRight < this.modelWidth - 1) {
				lastLonInterval = (this.getPointLonLat(gridRight + 1, 0).getLon() - lonLatRightBottom.getLon()) / 2;
			} else {
				lastLonInterval = this.defaultLonInterval / 2;
			}
			
			this.boundLonLat = new BoundLonLat(
					lonLatLeftTop.getLat() + firstLatInterval, 
					lonLatRightBottom.getLat() - lastLatInterval, 
					lonLatLeftTop.getLon() - firstLonInterval - this.lonShift, 
					lonLatRightBottom.getLon() + lastLonInterval - this.lonShift);
							
			this.latInterval = new double[this.getRows()];
				
			for(int i=gridTop ; i>=gridBottom ; i--) {
				
				if(i == gridTop) {	
					
					this.latInterval[gridTop - i] = 
							firstLatInterval + (this.getPointLonLat(0, i).getLat() - this.getPointLonLat(0, i-1).getLat()) / 2;
					
				} else if(i == gridBottom) {
					
					this.latInterval[gridTop - i] = 
							(this.getPointLonLat(0, i+1).getLat() - this.getPointLonLat(0, i).getLat()) / 2 + lastLatInterval;
					
				} else {
					this.latInterval[gridTop - i] = 
							(this.getPointLonLat(0, i+1).getLat() - this.getPointLonLat(0, i).getLat()) / 2 +
							(this.getPointLonLat(0, i).getLat() - this.getPointLonLat(0, i-1).getLat()) / 2;
				}
			}
			
			this.lonInterval = new double[this.getCols()];
			
			for(int i=gridLeft ; i <=gridRight ; i++) {
				
				if(i == gridLeft) {	
					
					this.lonInterval[i - gridLeft] = 
							firstLonInterval + (this.getPointLonLat(i+1, 0).getLon() - this.getPointLonLat(i, 0).getLon()) / 2;
					
				} else if(i == gridRight) {
					
					this.lonInterval[i - gridLeft] = 
							(this.getPointLonLat(i, 0).getLon() - this.getPointLonLat(i-1, 0).getLon()) / 2 + lastLonInterval;
					
				} else {
					this.lonInterval[i - gridLeft] = 
							(this.getPointLonLat(i+1, 0).getLon() - this.getPointLonLat(i, 0).getLon()) / 2 +
							(this.getPointLonLat(i, 0).getLon() - this.getPointLonLat(i-1, 0).getLon()) / 2;
				}
			}
		}
	}	
	
	public double[] getLatInterval() {
		return this.latInterval;
	}
	
	public double[] getLonInterval() {
		return this.lonInterval;
	}
	
	public BoundXY getBoundXY() {
		return this.boundXY;
	}
	
	public BoundLonLat getBoundLonLat() {
		return this.boundLonLat;
	}
	
	public int getRows() {
		return Math.abs(this.boundXY.getTop() - this.boundXY.getBottom()) + 1;
	}
	
	public int getCols() {
		return Math.abs(this.boundXY.getRight() - this.boundXY.getLeft()) + 1;
	}
	
	public List<Map<String, Object>> getPolygonDataList() {
		return this.polygonDataList;
	}
	
	private double NumberToFixed(double number) {		
		return Math.floor(number*Math.pow(10, this.numberFix))/Math.pow(10, this.numberFix);
	}
	
	public FloatBuffer getLatBuffer() {
		return this.latBuffer;
	}
	
	public FloatBuffer getLonBuffer() {
		return this.lonBuffer;
	}
}

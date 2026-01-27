package kama.daemon.common.util;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;

import kama.daemon.common.util.model.ModelGridUtil;
import kama.daemon.common.util.model.PointXY;
import ucar.ma2.InvalidRangeException;
import ucar.ma2.Range;
import ucar.nc2.Variable;
import ucar.nc2.dataset.NetcdfDataset;

import com.google.common.collect.ArrayListMultimap;

public class AMF {
	
	public static String createDigitalTaf(String storePath, String coordinatesLatPath, String coordinatesLonPath, Date ldapsPcMaxIssuedDt, double lat, double lon) {
	
		ModelGridUtil modelGridUtil = new ModelGridUtil(ModelGridUtil.Model.LDPS, null, coordinatesLatPath, coordinatesLonPath);
		PointXY pointXY = modelGridUtil.getPointXY(lon, lat);
		
		int arrayNumber = 37; 
		String[] Ztime = new String[arrayNumber];
		float[] u_wind = new float[arrayNumber];// units : m/s
		float[] v_wind = new float[arrayNumber];// units : m/s
		float[] wind_direction = new float[arrayNumber];// units : m/s
		float[] wind_speed = new float[arrayNumber];// units : m/s
		float[] visibility = new float[arrayNumber];// units : m
		
		float[] high_cloud = new float[arrayNumber];// units : %
		float[] medium_cloud = new float[arrayNumber];// units : %
		float[] low_cloud = new float[arrayNumber];// units : %
		
		float[] temperature = new float[arrayNumber];// units : K
		
		float[]  precipitation = new float[arrayNumber]; // units : kg.m-2 강수량
		float[]  snow = new float[arrayNumber]; // units : kg.m-2 강설량
		float[]  total_cloud = new float[arrayNumber]; //구름량
		float[]  height_cloud = new float[arrayNumber]; //구름높이
		
		// (Y,X)  시작배열 0번  443,354  1,1  439,342  464,533
		if(!readModelData(storePath, ldapsPcMaxIssuedDt, pointXY, Ztime, u_wind, v_wind, wind_direction,
				wind_speed, visibility, high_cloud, medium_cloud, low_cloud, temperature,
				precipitation, snow, total_cloud, height_cloud)) {
			return null;
		}
		
		int skip = 5;
		int tafLength = 31;
			
		// 풍향, 풍속 계산
		wind_direction_speed_calc(u_wind, v_wind, wind_direction, wind_speed, arrayNumber);
		
		u_wind = Arrays.copyOfRange(u_wind, skip, skip + tafLength);
		v_wind = Arrays.copyOfRange(v_wind, skip, skip + tafLength);
		wind_direction = Arrays.copyOfRange(wind_direction, skip, skip + tafLength);
		wind_speed = Arrays.copyOfRange(wind_speed, skip, skip + tafLength);
		visibility = Arrays.copyOfRange(visibility, skip, skip + tafLength);
		high_cloud = Arrays.copyOfRange(high_cloud, skip, skip + tafLength);
		medium_cloud = Arrays.copyOfRange(medium_cloud, skip, skip + tafLength);
		low_cloud = Arrays.copyOfRange(low_cloud, skip, skip + tafLength);
		temperature = Arrays.copyOfRange(temperature, skip, skip + tafLength);
		precipitation = Arrays.copyOfRange(precipitation, skip, skip + tafLength);
		snow = Arrays.copyOfRange(snow, skip, skip + tafLength);
		total_cloud = Arrays.copyOfRange(total_cloud, skip, skip + tafLength);
		height_cloud = Arrays.copyOfRange(height_cloud, skip, skip + tafLength);
		Ztime = Arrays.copyOfRange(Ztime, skip, skip + tafLength);
	
		//TAF 생성
		String messageTAF = "";
		messageTAF = IdentificationTAF(Ztime[0], lat, lon);//식별군
		messageTAF += GroundwindTAF(wind_direction, wind_speed, 0);//지상풍
		messageTAF += VisibilityTAF(visibility, high_cloud, medium_cloud, low_cloud, precipitation, snow, 0);//시정
		messageTAF += CloudTAF(low_cloud, height_cloud, 0);//구름
		messageTAF += temperatureTAF(temperature, Ztime);//기온
				
		//중복키 허용
		ArrayListMultimap<Object, Object> multimap = ArrayListMultimap.create();
		
		changeTAF(multimap, Ztime, wind_direction, wind_speed, visibility, high_cloud, medium_cloud, low_cloud, precipitation, snow );//변화군
		
		Iterator<Object> iter = multimap.keySet().iterator();
		ArrayList KEY = new ArrayList();
		while(iter.hasNext()) {
			KEY.add(iter.next()); 
		}
		Collections.sort(KEY);
				 
	    for(int i=0;i<KEY.size();i++){
	    	int cntData = multimap.get(KEY.get(i)).size();//데이터 갯수
	        List value = multimap.get(KEY.get(i));
	        Collections.sort(value);
	        for(int j=0;j<value.size();j++)
	        {
	        	messageTAF += "\r\n";
	        	messageTAF += value.get(j);
	        }
	    }
	    messageTAF += "=";
	    
	    return messageTAF;
	}
	
	public static void changeTAF(ArrayListMultimap<Object, Object> multimap, String[] Ztime,
			float[] wind_direction, float[] wind_speed,
			float[] visibility,
			float[] high_cloud, float[] medium_cloud, float[] low_cloud,
			float[] precipitation,
			float[] snow
			) {
		
		//fm
		boolean bsnow = false;
		boolean brain = false;
		int idx=0;
		
		//test code
//		for(int u=0;u<15;u++) {
//			snow[u] = 1.0f;
//		}
//		for(int u=10;u<31;u++) {
//			precipitation[u] = 1.0f;
//		}
		
		//눈이 계속 올경우
		for(int i=1;i<snow.length-1; i++) {
			if( (snow[i]==0.0 && snow[i+1]>0.0) )
			{
				bsnow = true;
				idx = i+1;
				for(int j=i+1;j<snow.length-1;j++) {
					if(snow[j]<=0.0) { bsnow = false; break; }
				}
				break;
			}
		}
		if(bsnow==true) {
			multimap.put(Ztime[idx], "FM" + Ztime[idx] + "00"+
					GroundwindTAF(wind_direction, wind_speed, idx) +
					VisibilityTAF(visibility, high_cloud, medium_cloud, low_cloud, precipitation, snow, idx));
			return;
		}
		//비가 계속 올경우
		for(int i=1;i<precipitation.length-1; i++) {
			if( (precipitation[i]==0.0 && precipitation[i+1]>0.0) )
			{
				brain = true;
				idx = i+1;
				for(int j=i+1;j<precipitation.length-1;j++) {
					if(precipitation[j]<=0.0) { brain = false; break; }
				}
				break;
			}
		}
		if(brain==true) {
			multimap.put(Ztime[idx], "FM" + Ztime[idx] + "00"+
					GroundwindTAF(wind_direction, wind_speed, idx) +
					VisibilityTAF(visibility, high_cloud, medium_cloud, low_cloud, precipitation, snow, idx));
			return;
		}
		
		//눈이 내리다 안 올 경우
		for(int i=1;i<snow.length-1; i++) {
			if( (snow[i]>0.0 && snow[i+1]==0.0) ) {
				bsnow = true;
				idx = i+1;
				for(int j=i+1;j<snow.length-1;j++) {
					if(snow[j]>0.0) { bsnow = false; break; }
				} break;
			}
		}
		if(bsnow==true) {
			multimap.put(Ztime[idx], "FM" + Ztime[idx] + "00"+
					GroundwindTAF(wind_direction, wind_speed, idx) +
					VisibilityTAF(visibility, high_cloud, medium_cloud, low_cloud, precipitation, snow, idx));
			//return;
		}
		
		//비가 오다 안 올 경우
		for(int i=1;i<precipitation.length-1; i++) {
			if( (precipitation[i]>0.0 && precipitation[i+1]==0.0) ) {
				brain = true;
				idx = i+1;
				for(int j=i+1;j<precipitation.length-1;j++) {
					if(precipitation[j]>0.0) { brain = false; break; }
				} break;
			}
		}
		if(brain==true) {
			multimap.put(Ztime[idx], "FM" + Ztime[idx] + "00"+
					GroundwindTAF(wind_direction, wind_speed, idx) +
					VisibilityTAF(visibility, high_cloud, medium_cloud, low_cloud, precipitation, snow, idx));
			//return;
		}
		
		
		//becmg
		for(int i=1;i<wind_direction.length-1; i++) {
			if(Math.abs(wind_direction[i]-wind_direction[i+1]) >= 60.0 || Math.abs(wind_speed[i]*2.0 - wind_speed[i+1]*2.0) >= 10.0)
			{	
				multimap.put(Ztime[i], "BECMG "+ Ztime[i]+"/"+Ztime[(i+2 >= Ztime.length ? i+1 : i+2)] +
						GroundwindTAF(wind_direction, wind_speed, i) +
						VisibilityTAF(visibility, high_cloud, medium_cloud, low_cloud, precipitation, snow, i));
			}
		}

		
		//tempo
		//precipitation[8] = 0.7f;
		//snow[21] = 2.0f;
		for(int i=1;i<wind_direction.length-1; i++) {
			if( (precipitation[i-1]==0.0 && precipitation[i]>0.0 && precipitation[i+1]==0.0) ||
				(snow[i-1]==0.0 && snow[i]>0.0 && snow[i+1]==0.0) )
			{
				multimap.put(Ztime[i], "TEMPO "+ Ztime[i]+"/"+Ztime[i+1] +
						VisibilityTAF(visibility, high_cloud, medium_cloud, low_cloud, precipitation, snow, i));
			}
		}
	}
	
	public static String temperatureTAF(float[] temperature, String[] Ztime) {
		
		String msgTaf="";
		
		CHisto histo = new CHisto();
		for(int i=0; i<24; i++) {
			histo.Add(temperature[i], i);
		}
		
		if(histo.max()-273.15 > 0.0) {
			msgTaf += String.format(" TX%02d/%sZ", (int)(histo.max()-273.15), Ztime[histo.idxmax()]);
		} else {
			msgTaf += String.format(" TXM%02d/%sZ", (int)(histo.max()-273.15), Ztime[histo.idxmax()]);
		}
		
		if(histo.min()-273.15 > 0.0) {
			msgTaf += String.format(" TN%02d/%sZ", (int)(histo.max()-273.15), Ztime[histo.idxmin()]);
		} else {
			msgTaf += String.format(" TNM%02d/%sZ", (int)(histo.max()-273.15), Ztime[histo.idxmin()]);
		}
		
		return msgTaf;
	}
	
	public static String CloudTAF(float[] low_cloud, float[] height_cloud, int idx) {
		
		String msgTaf="";
		int ft = (int)Math.round(height_cloud[idx]/100.0);
		float oktas = (float) (Math.round(low_cloud[idx] * 8.0));
		if(oktas >= 1.0 && oktas <= 2.0) {
			msgTaf += String.format(" FEW%03d", ft);
		} else if(oktas >= 3.0 && oktas <= 4.0) {
			msgTaf += String.format(" SCT%03d", ft);
		} else if(oktas >= 5.0 && oktas <= 7.0) {
			msgTaf += String.format(" BKN%03d", ft);
		}  else if(oktas == 8.0) {
			msgTaf += String.format(" OVC%03d", ft);
		}
		
		return msgTaf;
	}
	
	public static String VisibilityTAF(float[] visibility, float[] high_cloud, float[] medium_cloud, float[] low_cloud,
			float[] precipitation, float[] snow, int idx) {
		
		String msgTaf="";
		if(visibility[idx] >= 10000 && high_cloud[idx] == 0.0 && medium_cloud[idx] == 0.0 && low_cloud[idx] == 0.0 &&
				precipitation[idx] == 0.0 && snow[idx] == 0.0	) {
			msgTaf = " CAVOK";
			return msgTaf;
		}
		
		if(visibility[idx] < 800)
		{
			msgTaf += String.format(" %04d", (int)(Math.round(visibility[idx]/50.0)*50.0));
		} else if(visibility[idx] > 800 && visibility[idx] < 5000) {
			msgTaf += String.format(" %04d", (int)(Math.round(visibility[idx]/100.0)*100.0));
		}else if(visibility[idx] > 5000 && visibility[idx] < 10000) {
			msgTaf += String.format(" %04d", (int)(Math.round(visibility[idx]/1000.0)*1000.0));
		}else {
			msgTaf += " 9999";
		}
		
		if(precipitation[idx] > 0.0 || snow[idx] > 0.0){
			if(precipitation[idx] < snow[idx]) {
				if(snow[idx] < 1.0) {
					msgTaf += " -SN";
				} else if(snow[idx] > 10.0) {
					msgTaf += " +SN";
				} else {
					msgTaf += " SN";
				}
			} else {
				if(precipitation[idx] < 1.0) {
					msgTaf += " -RA";
				} else if(precipitation[idx] > 10.0) {
					msgTaf += " +RA";
				} else {
					msgTaf += " RA";
				}
			}
			return msgTaf;
		}
		
		if(visibility[idx] < 4800) {
			msgTaf += " FG";		
		} else if(visibility[idx] >= 4800 && visibility[idx] < 8000) {
			msgTaf += " BR";
		} else {
			return msgTaf;
		}
		
		return msgTaf;
	}
	
	public static String GroundwindTAF(float[] wind_direction, float[] wind_speed, int idx) {
		
		String msgTaf="";
		msgTaf += String.format(" %03d",(int)(Math.round(wind_direction[idx]/10.0)*10.0));
		
		CHisto chisto = new CHisto();
		for(int i=0;i<12;i++)
		{
			chisto.Add(wind_speed[i]);
		}
		if(chisto.mean()*2.0 < 5.0){
			msgTaf += "05KT";
			return msgTaf;
		}
		
		float averageWindSpeed = (float) (Math.round(chisto.mean()*2.0/5.0)*5.0);
		float maximumWindSpeed = (float) (Math.round(chisto.max()*2.0/5.0)*5.0);
		if(averageWindSpeed == maximumWindSpeed) averageWindSpeed -= 5.0;
		msgTaf += String.format("%02dG", (int)averageWindSpeed);
		msgTaf += String.format("%02dKT", (int)maximumWindSpeed);
		
		return msgTaf;
	}
	
	public static String IdentificationTAF(String ztime, double lat, double lon) {
		
		String msgTaf="TAF "+ Math.floor(lat*1000)/1000 + "/" + Math.floor(lon*1000)/1000;
		int day =  Integer.parseInt(ztime.substring(0, 2));
		int hour = Integer.parseInt(ztime.substring(2, 4));
		
		msgTaf += String.format(" %02d%02d00Z", day, hour);
		msgTaf += String.format(" %02d%02d/%02d%02d", day, hour+1, day+1, hour+7);
		
		return msgTaf;
	}
	
	public static void wind_direction_speed_calc(float[] u, float[] v, float[] dir, float[] speed, int cnt) {
		for(int i=0; i<cnt; i++) {
			speed[i] = (float) Math.sqrt(Math.pow(u[i], 2) + Math.pow(v[i], 2));// 풍속
			dir[i] = (float) ((Math.atan2(u[i], v[i]) * 180 / Math.PI) + 180.0);// 풍향
		}
	}
	
	public static boolean readModelData(String storePath, Date ldapsPcMaxIssuedDt, PointXY pointXY,
			String[] Ztime,
			float[] u_wind,
			float[] v_wind,
			float[] wind_direction,
			float[] wind_speed,
			float[] visibility,
			float[] high_cloud,
			float[] medium_cloud,
			float[] low_cloud,
			float[] temperature,
			float[] precipitation,
			float[] snow,
			float[] total_cloud,
			float[] height_cloud
			)
	{
		
		String ldapsPcPath = storePath + "/UM_LOA_PC/" + new SimpleDateFormat("yyyy/MM/dd/HH").format(ldapsPcMaxIssuedDt);
		
		File[] ldapsPcFileList = new File(ldapsPcPath).listFiles(new FilenameFilter(){

			@Override
			public boolean accept(File dir, String name) {
				
				if(name.matches("qwumloa_pc[0-9]{3}.nc")) {
					return true;
				} 
				
				return false;
			}
		});
		
		Arrays.sort(ldapsPcFileList);
		
		if(ldapsPcFileList.length != 19) {
			return false;
		}
		
		Calendar cal = new GregorianCalendar();
		cal.setTime(ldapsPcMaxIssuedDt);
		
		int timeCnt = 0;
		
		for(int i=0 ; i<ldapsPcFileList.length ; i++) {
			
			File ldapsPcFile = ldapsPcFileList[i];
			
			int timeLength = i == 0 ? 0 : 1;
			
			String[] variables = {
				"x-wind", "y-wind", // 바람
				"field25", // 시정
				// 현천
				"field31", "field32", "field33", // 구름(High, Medium, Low)
				"temp", // 기온
				"lsrain", //강수량
				"lssnow", //강설량
				"field30", //구름량
				"field75" //구름높이
			};
			
			for(int j=0 ; j<variables.length; j++) {
				
				float[] value = getLdapsPcData(ldapsPcFile.getAbsolutePath(), pointXY, timeLength, variables[j]);
					
				for(int k=0 ; k<timeLength+1 ; k++) {		
									
					switch(j) {
						case 0: u_wind[timeCnt+k] = value[k]; break;
						case 1: v_wind[timeCnt+k] = value[k]; break;
						case 2: visibility[timeCnt+k] = value[k]; break;
						case 3: high_cloud[timeCnt+k] = value[k]; break;
						case 4: medium_cloud[timeCnt+k] = value[k]; break;
						case 5: low_cloud[timeCnt+k] = value[k]; break;
						case 6: temperature[timeCnt+k] = value[k]; break;
						case 7: precipitation[timeCnt+k] = value[k]; break;
						case 8: snow[timeCnt+k] = value[k]; break;
						case 9: total_cloud[timeCnt+k] = value[k]; break;
						case 10: height_cloud[timeCnt+k] = value[k]; break;
					}
				}
			}
			
			for(int k=0 ; k<timeLength+1 ; k++) {
				
				Ztime[timeCnt+k] = new SimpleDateFormat("ddHH").format(cal.getTime());
				
				cal.add(Calendar.HOUR_OF_DAY, 1);
			}
			
			timeCnt += timeLength + 1;
		}
		
		return true;
	}
	
	public static float[] getLdapsPcData(String filePath, PointXY pointXY, int timeLength, String variable) {
		
		System.out.println(":: READ FILE -> " + filePath);
		
		if(!new File(filePath).exists()) {
			return null;
		}
		
		NetcdfDataset ncFile = null;
		
		try {
		
			ncFile = NetcdfDataset.acquireDataset(filePath, null);
			
			Variable var = ncFile.findVariable(variable);
			
			List<Range> rangeList = new ArrayList<Range>();
			
			rangeList.add(new Range(0, timeLength));
			rangeList.add(new Range(0, 0));
				
			rangeList.add(new Range(pointXY.getY(), pointXY.getY()));
			rangeList.add(new Range(pointXY.getX(), pointXY.getX()));
			
			float[] storage = (float[])var.read(rangeList).getStorage();
			
			return storage;
			
		} catch (InvalidRangeException | IOException | NullPointerException e) {
			
			e.printStackTrace();
			
			return null;
			
		} finally {
    		
			try {
				ncFile.close();
			} catch (Exception e) {}
		}
	}
	
	public static class CHisto
    {
        private double sum, min, max;
        private int n, idxmin, idxmax;

        public CHisto()
        {
            n = 0;
            idxmin = 0;
            idxmax = 0;
            sum = 0.0;
            min = 9999.0;
            max = -9999.0;
        }

        public void Add(double val)
        {
            n++;
            sum += val;
            
            if(min > val) { min = val;}
            if(max < val) {max = val;}
        }
        
        public void Add(double val, int idx)
        {
            n++;
            sum += val;
            
            if(min > val) { min = val; idxmin = idx;}
            if(max < val) {max = val; idxmax = idx;}
        }

        public double mean()
        {
            return sum / (double)n;
        }
        
        public double min()
        {
            return min;
        }
        
        public double max()
        {
            return max;
        }
        
        public int idxmin()
        {
            return idxmin;
        }
        
        public int idxmax()
        {
            return idxmax;
        }
        
        public void Clear()
        {
            n = 0;
            sum = 0.0;
            max = 0.0;
        }
    }
}


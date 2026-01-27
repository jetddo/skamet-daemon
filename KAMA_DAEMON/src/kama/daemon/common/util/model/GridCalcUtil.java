package kama.daemon.common.util.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;

public class GridCalcUtil {

	public static double[] getLatitudeRatioList(double top, double bottom, int y, int scaleFactorY) {
    	
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
    
	public static double[] calculateCumulativeArr(double[] arr) {
    	
    	double[] cumulativeArr = new double[arr.length+1];
    	
    	cumulativeArr[0] = 0;
    	
    	double total = 0;
    	
    	for(int i=0 ; i<arr.length ; i++) {
    		total += arr[i];
    		cumulativeArr[i+1] = total;
    	}
    	
    	return cumulativeArr;
    }
	
	public static Float[][] convertStorageToValues(Object storage, int rows, int cols) {
    	
    	Float[][] values = new Float[rows][cols];
    	
    	if(storage instanceof float[]) {
    		
    		for(int i=0 ; i<rows ; i++) {
    			for(int j=0 ; j<cols ; j++) {
    				
    				Float f = ((float[])storage)[i*cols + j];
    				
    				if(!f.isNaN()) {
    					values[i][j] = f;	
    				} else {
    					values[i][j] = -999.0f;
    				}
    			}
    		}	
    		
    	} else if(storage instanceof double[]) {
    		
    		for(int i=0 ; i<rows ; i++) {
    			for(int j=0 ; j<cols ; j++) {
    				
    				Float f = (float)((double[])storage)[i*cols + j];
    				
    				if(!f.isNaN()) {
    					values[i][j] = f;	
    				} else {
    					values[i][j] = -999.0f;
    				}
    			}
    		}	
    		
    	} else if(storage instanceof short[]) {
    		
    		for(int i=0 ; i<rows ; i++) {
    			for(int j=0 ; j<cols ; j++) {
    				
    				Float f = (float)((short[])storage)[i*cols + j];
    				
    				if(!f.isNaN()) {
    					values[i][j] = f;	
    				} else {
    					values[i][j] = -999.0f;
    				}
    			}
    		}
    	}
		
		return values;
    }
	
	public static Float[][] convertStorageToValuesReverse(Object storage, int rows, int cols) {
    	
    	Float[][] values = new Float[rows][cols];
    	
    	if(storage instanceof float[]) {
    		
    		for(int i=0 ; i<rows ; i++) {
    			for(int j=0 ; j<cols ; j++) {
    				
    				Float f = ((float[])storage)[i*cols + j];
    				
    				if(!f.isNaN()) {
    					values[rows-i-1][j] = f;	
    				} else {
    					values[rows-i-1][j] = -999.0f;
    				}
    			}
    		}	
    		
    	} else if(storage instanceof double[]) {
    		
    		for(int i=0 ; i<rows ; i++) {
    			for(int j=0 ; j<cols ; j++) {
    				
    				Float f = (float)((double[])storage)[i*cols + j];
    				
    				if(!f.isNaN()) {
    					values[rows-i-1][j] = f;	
    				} else {
    					values[rows-i-1][j] = -999.0f;
    				}
    			}
    		}	
    	} else if(storage instanceof double[]) {
    		
    		for(int i=0 ; i<rows ; i++) {
    			for(int j=0 ; j<cols ; j++) {
    				
    				Float f = (float)((short[])storage)[i*cols + j];
    				
    				if(!f.isNaN()) {
    					values[rows-i-1][j] = f;	
    				} else {
    					values[rows-i-1][j] = -999.0f;
    				}
    			}
    		}	
    	}
		
		return values;
    }
}

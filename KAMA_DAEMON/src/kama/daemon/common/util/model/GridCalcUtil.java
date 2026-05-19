package kama.daemon.common.util.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;

import ucar.nc2.Attribute;
import ucar.nc2.Variable;

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
    				
    				float f = ((float[])storage)[i*cols + j];
    				
    				if(!Float.isNaN(f)) {
    					values[i][j] = f;	
    				} else {
    					values[i][j] = -999.0f;
    				}
    			}
    		}	
    		
    	} else if(storage instanceof double[]) {
    		
    		for(int i=0 ; i<rows ; i++) {
    			for(int j=0 ; j<cols ; j++) {
    				
    				float f = (float)((double[])storage)[i*cols + j];
    				
    				if(!Float.isNaN(f)) {
    					values[i][j] = f;	
    				} else {
    					values[i][j] = -999.0f;
    				}
    			}
    		}	
    		
    	} else if(storage instanceof short[]) {
    		
    		for(int i=0 ; i<rows ; i++) {
    			for(int j=0 ; j<cols ; j++) {
    				
    				float f = (float)((short[])storage)[i*cols + j];
    				
    				if(!Float.isNaN(f)) {
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
    				
    				float f = ((float[])storage)[i*cols + j];
    				
    				if(!Float.isNaN(f)) {
    					values[rows-i-1][j] = f;	
    				} else {
    					values[rows-i-1][j] = -999.0f;
    				}
    			}
    		}	
    		
    	} else if(storage instanceof double[]) {
    		
    		for(int i=0 ; i<rows ; i++) {
    			for(int j=0 ; j<cols ; j++) {
    				
    				float f = (float)((double[])storage)[i*cols + j];
    				
    				if(!Float.isNaN(f)) {
    					values[rows-i-1][j] = f;	
    				} else {
    					values[rows-i-1][j] = -999.0f;
    				}
    			}
    		}	
    	} else if(storage instanceof short[]) {
    		
    		for(int i=0 ; i<rows ; i++) {
    			for(int j=0 ; j<cols ; j++) {
    				
    				float f = (float)((short[])storage)[i*cols + j];
    				
    				if(!Float.isNaN(f)) {
    					values[rows-i-1][j] = f;	
    				} else {
    					values[rows-i-1][j] = -999.0f;
    				}
    			}
    		}	
    	}
		
		return values;
    }
	
	public static float[][] convertStorageToPrimitiveValues(Object storage, int rows, int cols) {
    	
    	float[][] values = new float[rows][cols];
    	
    	if(storage instanceof float[]) {
    		
    		for(int i=0 ; i<rows ; i++) {
    			for(int j=0 ; j<cols ; j++) {
    				
    				float f = ((float[])storage)[i*cols + j];
    				
    				if(!Float.isNaN(f)) {
    					values[i][j] = f;	
    				} else {
    					values[i][j] = -999.0f;
    				}
    			}
    		}	
    		
    	} else if(storage instanceof double[]) {
    		
    		for(int i=0 ; i<rows ; i++) {
    			for(int j=0 ; j<cols ; j++) {
    				
    				float f = (float)((double[])storage)[i*cols + j];
    				
    				if(!Float.isNaN(f)) {
    					values[i][j] = f;	
    				} else {
    					values[i][j] = -999.0f;
    				}
    			}
    		}	
    		
    	} else if(storage instanceof short[]) {
    		
    		for(int i=0 ; i<rows ; i++) {
    			for(int j=0 ; j<cols ; j++) {
    				
    				float f = (float)((short[])storage)[i*cols + j];
    				
    				if(!Float.isNaN(f)) {
    					values[i][j] = f;	
    				} else {
    					values[i][j] = -999.0f;
    				}
    			}
    		}
    	}
		
		return values;
    }
	
	public static float[][] convertStorageToPrimitiveValuesReverse(Object storage, int rows, int cols) {
    	
    	float[][] values = new float[rows][cols];
    	
    	if(storage instanceof float[]) {
    		
    		for(int i=0 ; i<rows ; i++) {
    			for(int j=0 ; j<cols ; j++) {
    				
    				float f = ((float[])storage)[i*cols + j];
    				
    				if(!Float.isNaN(f)) {
    					values[rows-i-1][j] = f;	
    				} else {
    					values[rows-i-1][j] = -999.0f;
    				}
    			}
    		}	
    		
    	} else if(storage instanceof double[]) {
    		
    		for(int i=0 ; i<rows ; i++) {
    			for(int j=0 ; j<cols ; j++) {
    				
    				float f = (float)((double[])storage)[i*cols + j];
    				
    				if(!Float.isNaN(f)) {
    					values[rows-i-1][j] = f;	
    				} else {
    					values[rows-i-1][j] = -999.0f;
    				}
    			}
    		}	
    	} else if(storage instanceof short[]) {
    		
    		for(int i=0 ; i<rows ; i++) {
    			for(int j=0 ; j<cols ; j++) {
    				
    				float f = (float)((short[])storage)[i*cols + j];
    				
    				if(!Float.isNaN(f)) {
    					values[rows-i-1][j] = f;	
    				} else {
    					values[rows-i-1][j] = -999.0f;
    				}
    			}
    		}	
    	}
		
		return values;
    }
	
	public static float[][] convertStorageToPrimitiveValuesFromAttr(Variable var, Object storage, int rows, int cols) {
		
	    float[][] values = convertStorageToPrimitiveValues(storage, rows, cols);

	    applyScaleOffsetFromAttr(values, var, rows, cols);

	    return values;
	}
	
	public static float[][] convertStorageToPrimitiveValuesReverseFromAttr(Variable var, Object storage, int rows, int cols) {
		
	    float[][] values = convertStorageToPrimitiveValuesReverse(storage, rows, cols);

	    applyScaleOffsetFromAttr(values, var, rows, cols);

	    return values;
	}
	
	private static void applyScaleOffsetFromAttr(float[][] values, Variable var, int rows, int cols) {
		
	    float scale = (float) getDoubleAttr(var, "scale_factor", 1.0);
	    float offset = (float) getDoubleAttr(var, "add_offset", 0.0);

	    for (int i = 0; i < rows; i++) {
	        for (int j = 0; j < cols; j++) {
	            if (!Float.isNaN(values[i][j]) && values[i][j] != -999.0f) {
	                values[i][j] = values[i][j] * scale + offset;
	            }
	        }
	    }
	}
	
	public static double getDoubleAttr(Variable var, String attrName, double defaultValue) {

	    Attribute attr = var.findAttribute(attrName);

	    if(attr == null) {
	        return defaultValue;
	    }

	    Number value = attr.getNumericValue();

	    if(value == null) {
	        return defaultValue;
	    }

	    return value.doubleValue();
	}
}

package kama.daemon.model.prediction.adopt.LDAPS_ISOB.loader;

import kama.daemon.common.util.Log;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chlee
 * Created on 2017-01-16.
 */
public class CityInfo
{
    public int stnid;
    public int xgrdidx, ygrdidx;
    public double glat, glon;
    public double olat, olon;
    public double grass;
    public double urban;
    public double water;
    public String ecity;
    public String kcity;

    public static List<CityInfo> loadCityInfo(String filePath) throws IOException
    {
        List<String> lines = Files.readAllLines(new File(filePath).toPath(), Charset.defaultCharset());
        List<CityInfo> cityList = new ArrayList<>();

        for (int i = 2; i < lines.size(); i++)
        {
            String line = lines.get(i);
            CityInfo cityInfo;

            // Skip if invalid line
            if (line.trim().length() < 1)
            {
                continue;
            }

            cityInfo = new CityInfo();

            // FORMAT(5X,A5,8X,4F10.5,1X,3F6.3,2X,A20,A11)
            int pos = 5;    // 5X
            cityInfo.stnid = Integer.parseInt(line.substring(pos, pos + 5));

            pos += 5; // A5
            pos += 8;
            cityInfo.glat = Double.parseDouble(line.substring(pos, pos + 10));
            pos += 10;
            cityInfo.glon = Double.parseDouble(line.substring(pos, pos + 10));
            pos += 10;
            cityInfo.olat = Double.parseDouble(line.substring(pos, pos + 10));
            pos += 10;
            cityInfo.olon = Double.parseDouble(line.substring(pos, pos + 10));
            pos += 10;
            pos += 1;  // 1X
            cityInfo.grass = Double.parseDouble(line.substring(pos, pos + 6));
            pos += 6;
            cityInfo.urban = Double.parseDouble(line.substring(pos, pos + 6));
            pos += 6;
            cityInfo.water = Double.parseDouble(line.substring(pos, pos + 6));
            pos += 6;
            pos += 2;
            cityInfo.ecity = line.substring(pos, pos + 20);
            pos += 20;
            cityInfo.kcity = line.substring(pos);

            cityList.add(cityInfo);
        }

        return cityList;
    }
}
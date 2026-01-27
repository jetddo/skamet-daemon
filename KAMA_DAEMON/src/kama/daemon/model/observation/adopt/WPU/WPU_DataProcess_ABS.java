package kama.daemon.model.observation.adopt.WPU;

import kama.daemon.common.db.DataProcessor;
import kama.daemon.common.util.DaemonException;
import kama.daemon.common.util.DaemonSettings;
import kama.daemon.common.util.DateFormatter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chlee
 * Created on 2016-12-09.
 */
public abstract class WPU_DataProcess_ABS extends DataProcessor
{
    public WPU_DataProcess_ABS(DaemonSettings settings, String prefix)
    {
        super(settings, prefix);
    }

    protected List<WindProfiler> parseFile(File file) throws IOException, ParseException
    {
        String line;
        String[] token;

        // Declare your column variables
        List<WindProfiler> lstWindProfiler;
        WindProfiler wf;
        List<WindProfiler> subRecords;
        String windData;

        lstWindProfiler = new ArrayList<WindProfiler>();

        try (BufferedReader br = new BufferedReader(new FileReader(file)))
        {
            // HEADER 파싱
            while ((line = br.readLine()) != null)
            {
                // token : #
                token = line.split("#");
                wf = new WindProfiler();

                // 관측시각
                wf.RecordTime = DateFormatter.parseDate(
                        String.format("%s-%s-%s %s:%s", token[3], token[4], token[5], token[6], token[7]),
                        "yyyy-M-d h:m"
                );

                wf.WMO_LocationCode = Integer.parseInt(token[2]);
                wf.Latitude = Double.parseDouble(token[8]) / 100;
                wf.Longitude = Double.parseDouble(token[9]) / 100;

                // WIND 파싱
                windData = br.readLine();
                subRecords = retrieveWindData(wf, windData);

                lstWindProfiler.addAll(subRecords);

                // We only need wind data. Throw away rest of the data.
                break;
            }
        }

        return lstWindProfiler;
    }

    protected String[] convertToRecordFormat(WindProfiler windProfiler)
    {
        List<String> lstTokens;
        String[] tokens;

        lstTokens = new ArrayList<String>();

        lstTokens.add(convertToDBText(windProfiler.RecordTime));
        lstTokens.add(convertToDBText(windProfiler.WMO_LocationCode));
        lstTokens.add(convertToDBText(windProfiler.Height));
        lstTokens.add(convertToDBText(windProfiler.Latitude));
        lstTokens.add(convertToDBText(windProfiler.Longitude));
        lstTokens.add(convertToDBText(windProfiler.WindDirection));
        lstTokens.add(convertToDBText(windProfiler.WindSpeed));
        lstTokens.add(convertToDBText(windProfiler.DataStatus));
        lstTokens.add(convertToDBText(windProfiler.IndexSequence));

        tokens = new String[lstTokens.size()];

        return lstTokens.toArray(tokens);
    }

    private List<WindProfiler> retrieveWindData(WindProfiler windProfiler, String txtData)
    {
        WindProfiler wf;
        List<WindProfiler> subRecords;
        String[] token;
        int count;
        int index;

        subRecords = new ArrayList<WindProfiler>();
        token = txtData.split("#");
        count = Integer.parseInt(token[1]);
        index = 0;

        // Each wind record contains 7 tokens
        for (int i = 2; i < token.length; i += 7)
        {
            // Only if full record exists
            if (i + 7 < token.length)
            {
                wf = (WindProfiler) windProfiler.clone();
                wf.Height = Double.parseDouble(token[i]);
                wf.WindDirection = Double.parseDouble(token[i + 1]);
                wf.WindSpeed = Double.parseDouble(token[i + 2]);
                wf.DataStatus = Integer.parseInt(token[i + 6]);
                wf.IndexSequence = index++;
                subRecords.add(wf);
            }
        }

        if (count != subRecords.size())
        {
            throw new DaemonException("Parsed size does not equal to the actual size.");
        }

        return subRecords;
    }
}
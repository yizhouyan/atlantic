package org.verdictdb.privacy;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DPRelatedTableMetaDataSet {
    private VerdictDBLogger log = VerdictDBLogger.getLogger(this.getClass());
    Map<Pair<String, String>, DPRelatedTableMetaData> dpRelatedMetaDataSet = new HashMap<>();

    public DPRelatedTableMetaDataSet(Map<Pair<String, String>, DPRelatedTableMetaData> dpRelatedMetaDataSet){
        this.dpRelatedMetaDataSet = dpRelatedMetaDataSet;
    }

    public static DPRelatedTableMetaDataSet createFromScrambleMetaSet(ScrambleMetaSet scrambleMetaSet) {
        Iterator<ScrambleMeta> it = scrambleMetaSet.iterator();
        Map<Pair<String, String>, DPRelatedTableMetaData> dpRelatedMetaDataSet = new HashMap<>();
        while(it.hasNext()){
            ScrambleMeta scrambleMeta = it.next();
            DPRelatedTableMetaData dpMetaData =  new DPRelatedTableMetaData(scrambleMeta);
            dpRelatedMetaDataSet.put(Pair.of(scrambleMeta.getSchemaName(), scrambleMeta.getTableName()),dpMetaData);
            dpRelatedMetaDataSet.put(Pair.of(scrambleMeta.getOriginalSchemaName(), scrambleMeta.getOriginalTableName()), dpMetaData);
        }
        return new DPRelatedTableMetaDataSet(dpRelatedMetaDataSet);
    }
    public boolean containsTable(Pair<String, String> tableSchemaName){
        return dpRelatedMetaDataSet.containsKey(tableSchemaName);
    }
    public DPRelatedTableMetaData getDPMetaForTable(Pair<String, String> tableSchemaName){
        return dpRelatedMetaDataSet.get(tableSchemaName);
    }
}

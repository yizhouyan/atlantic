package org.verdictdb.core.scrambling;

import com.fasterxml.jackson.annotation.*;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.exception.VerdictDBException;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;


public class PrivacyMeta implements Serializable {
    private static final long serialVersionUID = -5868149128257905562L;
    public Map<String, BigDecimal> columnMin = new HashMap<>();
    public Map<String, BigDecimal> columnMax = new HashMap<>();
    public Map<String, BigDecimal> privacyMetaMaxFreq = new HashMap<>();

    @JsonIgnore
    private VerdictDBLogger log = VerdictDBLogger.getLogger(this.getClass());

    public PrivacyMeta() {
        super();
    }

    public List<ExecutableNodeBase> getStatisticsNode(
            String oldSchemaName,
            String oldTableName,
            String columnMetaTokenKey,
            List<Pair<String, String>> columnNamesAndTypes) {
        List<ExecutableNodeBase> statisticsNodes = new ArrayList<>();
        PrivacyColMinMaxRetrievalNode countNode =
                new PrivacyColMinMaxRetrievalNode(oldSchemaName, oldTableName, columnMetaTokenKey);
        statisticsNodes.add(countNode);
        for (Pair<String, String> nameAndType : columnNamesAndTypes) {
            String colName = nameAndType.getLeft();
            PrivacyMaxFreqRetrievalNode maxFreqNode = new PrivacyMaxFreqRetrievalNode(oldSchemaName, oldTableName, colName);
            statisticsNodes.add(maxFreqNode);
        }
        return statisticsNodes;
    }

    public void extractPrivacyMeta(Map<String, Object> metaData) throws VerdictDBException {
        PrivacyColMinMaxRetrievalNode.extractMinMaxPrivacyMeta(metaData, columnMin, columnMax);
        PrivacyMaxFreqRetrievalNode.extractMaxFreqPrivacyMeta(metaData,privacyMetaMaxFreq);
    }
}

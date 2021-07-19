package org.verdictdb.privacy;

import org.verdictdb.VerdictSingleResult;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.*;

import java.util.HashMap;
import java.util.List;

/**
 * Process the query, and attach noise to the ResultSet based on differential privacy.
 */
public class DPDriver {
    protected VerdictDBLogger log = VerdictDBLogger.getLogger(this.getClass());

    private DPNoiseEstimator noiseEstimator;

    private HashMap<Integer, ColumnOp> aggregationColumns;
    private boolean isSupported;
    private VerdictOption options;

    public DPDriver(SelectQuery originalQuery, ScrambleMetaSet scrambleMetaSet,
                    DPRelatedTableMetaDataSet dpRelatedTableMetaDataSet, VerdictOption options) {
        isSupported = isQuerySupported(originalQuery);
        if (isSupported) {
            AbstractRelation table = originalQuery.getFromList().get(0);
            if (table instanceof BaseTable) {
                noiseEstimator = new BaseTableNoiseEstimator(originalQuery, scrambleMetaSet,
                        aggregationColumns, dpRelatedTableMetaDataSet, options);
            } else if (table instanceof JoinTable) {
                noiseEstimator = new JoinTableNoiseEstimator(originalQuery, scrambleMetaSet,
                        aggregationColumns, dpRelatedTableMetaDataSet, options);
            }
        }
        this.options=options;
        log.debug("Differential Privacy supported = " + getIsSupported() + ", enabled = " + options.isPrivacyEnabled());
    }

    private boolean getIsSupported() {
        return isSupported && (noiseEstimator != null) && noiseEstimator.isSupported;
    }

    private boolean isQuerySupported(SelectQuery originalQuery) {
        List<AbstractRelation> tableList = originalQuery.getFromList();
        if (tableList.size() != 1)
            return false;
        List<SelectItem> selectItems = originalQuery.getSelectList();
        // Queries with select * are not supported
        for (SelectItem item : selectItems) {
            if (item instanceof AsteriskColumn) {
                return false;
            }
        }
        aggregationColumns = new HashMap<Integer, ColumnOp>();
        for (int index = 0; index < selectItems.size(); index++) {
            SelectItem item = selectItems.get(index);
            if (item instanceof AliasedColumn) {
                item = ((AliasedColumn) item).getColumn();
            }
            if (item instanceof ColumnOp && ((ColumnOp) item).isPrivacySupportedAggregateColumn()) {
                aggregationColumns.put(index, (ColumnOp) item);
            }
        }
        log.debug(String.format("Found %d aggregation columns: %s", aggregationColumns.size(), aggregationColumns));
        if (aggregationColumns.size() == 0)
            return false;
        return true;
    }

    public VerdictSingleResult addNoiseToSingleResult(VerdictSingleResult result) {
        if (noiseEstimator == null) {
            log.debug("DP not supported in current query, return the original result. ");
            return result;
        }
        return noiseEstimator.addNoiseToSingleResult(result);
    }
}

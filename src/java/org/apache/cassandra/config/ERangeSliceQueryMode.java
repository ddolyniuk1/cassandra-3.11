package org.apache.cassandra.config;

import org.apache.cassandra.db.ConsistencyLevel;

public enum ERangeSliceQueryMode {
    None,
    Auto,
    LocalDC,
    LocalNode;

    /**
     * Get a new Mode from the consistency we provide it
     * @param consistency the consistency of the query requested
     * @return If the current mode is Auto - return a mode corresponding to consistency rules, 
     * otherwise return the existing value
     */
    public ERangeSliceQueryMode applyConsistency(ConsistencyLevel consistency) {
        if(this != Auto) {
            return this;
        }
        switch (consistency) {
            case LOCAL_SERIAL:
            case LOCAL_QUORUM:
            case LOCAL_ONE:
                return ERangeSliceQueryMode.LocalDC;
            default:
                return ERangeSliceQueryMode.None;
        }
    }
}

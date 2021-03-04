/*
// $Id$
// Aspen dataflow server
// Copyright (C) 2009-2020 SQLstream, Inc.
*/
package com.sqlstream.plugin.streamstats;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.logging.Logger;

import com.sqlstream.jdbc.StreamingPreparedStatement;
import com.sqlstream.jdbc.StreamingResultSet;
import com.sqlstream.jdbc.StreamingResultSet.RowEvent;
import com.sqlstream.plugin.impl.AbstractBaseUdx;

/**
 * The Throughput UDX
 */
public class StreamStats extends AbstractBaseUdx {
    
    public static final Logger tracer =
        Logger.getLogger(StreamStats.class.getName());

    private StreamStats(Logger tracer, ResultSet inputRows,
        PreparedStatement results)
        throws SQLException
    {
        super(tracer, inputRows, results);
        if (!(inputRows instanceof StreamingResultSet)
            || !inputRows.getMetaData().getColumnName(1).equals("ROWTIME")) {
            throw new SQLException("ResultSet parameter of this function needs to "
                + "be an instance of StreamingResultSet");
        }
        
    }

    /**
     * UDX that counts number of rows per second
     * The UDX accepts a cursor to a streaming query and emits rows per second based
     * based on ROWTIME.
     *
     * @param inputRows Used to retrieve incoming rows.
     * @param results Output row of type:
     *         (ROWTIME TIMESTAMP NOT NULL, rps  BIGINT)
     * @throws SQLException
     */
    public static void getStats(ResultSet inputRows, PreparedStatement results) throws SQLException
    {
        StreamStats udx = new StreamStats(tracer, inputRows, results);
        try {
            udx.execute();
        } catch (SQLException sqle) {
            if (!results.isClosed()) {
                throw sqle;
            }
        }
    }
    
    public void execute() throws SQLException {    
        StreamingResultSet in = (StreamingResultSet)inputRows;
        StreamingPreparedStatement out = (StreamingPreparedStatement)results;
        long prevSecond = 0;
    	long rowcount = 0;
    	long boundcount = 0;
    	long timeoutcount = 0;
    	long currentMillis = 0;
    	long currentSecond = 0;

        while (!out.isClosed()) {
            RowEvent e = in.nextRowOrRowtime(1000);
            switch (e) {
            case EndOfStream:
                return;  // end of input
            case Timeout:

		timeoutcount++;
		// what time should we use here?
		// just ignore for now; we will only count timeouts when we see rowtime or rt bound

                break;  // timeout ??

            case NewRow:
		rowcount++;

                currentMillis = in.getTimestampInMillis(1);  // rowtime
                break;

            case NewRowtimeBound:
		boundcount++;
		currentMillis = in.getRowtimeBound().getTime(); // rowtime bound time
            }

	    currentSecond = currentMillis / 1000;

            if (currentSecond != prevSecond) {
                    out.setTimestampInMillis(1, currentSecond * 1000);
                    out.setLong(2, rowcount);
                    out.setLong(3, boundcount);
		    out.setLong(4, timeoutcount);
                    out.executeUpdate();
                    rowcount = 0;
		    boundcount = 0;
		    timeoutcount = 0;
                    prevSecond = currentSecond;
            }

        }
    }
}

// End StreamStats.java

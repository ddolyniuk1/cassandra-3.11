package org.apache.cassandra.tools.nodetool;

import io.airlift.command.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(name = "flush-cdc",
        description = "Force CDC commit log segments to rotate and move to cdc_raw")
public class FlushCDC extends NodeTool.NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        try
        {
            probe.forceCDCFlush();
            System.out.println("CDC segments flushed to cdc_raw");
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error flushing CDC segments", e);
        }
    }
}

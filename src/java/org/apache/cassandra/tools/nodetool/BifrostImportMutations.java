package org.apache.cassandra.tools.nodetool;

import io.airlift.command.Command;
import io.airlift.command.Option;
import org.apache.cassandra.threesi.bifrost.services.BifrostImporterServiceMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

@Command(name = "bifrost-import", description = "Import mutations provided by the Bifrost hub.")
public class BifrostImportMutations extends NodeTool.NodeToolCmd {

    @Option(title = "File Path", name = {"-f", "--file-path"}, description = "Location of the mutation data to import", required = true)
    private String filePath;

    @Option(title = "Limit", name = {"-l", "--limit"}, description = "Set a maximum number of mutations to import.")
    private int limit = 0;

    @Option(title = "Keyspace Filter", name = {"-kf", "--keyspace-filter"}, description = "Set a filter to only import certain keyspaces.")
    private String keyspaceFilter;

    @Option(title = "Table Filter", name = {"-tf", "--table-filter"}, description = "Set a filter to only import certain tables.")
    private String tableFilter;

    @Override
    protected void execute(NodeProbe probe) {
        try {
            MBeanServerConnection mbsc = probe.getJmxc().getMBeanServerConnection();
            BifrostImporterServiceMBean importer = JMX.newMBeanProxy(
                    mbsc,
                    new ObjectName("org.apache.cassandra.threesi:type=BifrostImporter"),
                    BifrostImporterServiceMBean.class
            );
            importer.importMutations(filePath, limit, keyspaceFilter, tableFilter);
        } catch (Exception e) {
            probe.output().out.println("Failed to invoke Bifrost import: " + e.getClass().getName() + ": " + e.getMessage());
            if (e.getCause() != null) {
                probe.output().out.println("Caused by: " + e.getCause().getClass().getName() + ": " + e.getCause().getMessage());
            }
            e.printStackTrace(probe.output().out);
        }
    }
}
package org.embulk.output;

import java.util.Properties;
import java.io.IOException;
import java.sql.SQLException;
import org.slf4j.Logger;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import org.embulk.spi.Exec;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.redshift.RedshiftOutputConnector;
import org.embulk.output.redshift.RedshiftCopyBatchInsert;

public class RedshiftOutputPlugin
        extends AbstractJdbcOutputPlugin
{
    private final Logger logger = Exec.getLogger(RedshiftOutputPlugin.class);

    public interface RedshiftPluginTask extends PluginTask
    {
        @Config("host")
        public String getHost();

        @Config("port")
        @ConfigDefault("5439")
        public int getPort();

        @Config("user")
        public String getUser();

        @Config("password")
        @ConfigDefault("\"\"")
        public String getPassword();

        @Config("database")
        public String getDatabase();

        @Config("schema")
        @ConfigDefault("\"public\"")
        public String getSchema();

        @Config("access_key_id")
        public String getAccessKeyId();

        @Config("secret_access_key")
        public String getSecretAccessKey();

        @Config("iam_user_name")
        public String getIamUserName();

        @Config("s3_bucket")
        public String getS3Bucket();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return RedshiftPluginTask.class;
    }

    @Override
    protected RedshiftOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation)
    {
        RedshiftPluginTask t = (RedshiftPluginTask) task;

        String url = String.format("jdbc:postgresql://%s:%d/%s",
                t.getHost(), t.getPort(), t.getDatabase());

        Properties props = new Properties();
        props.setProperty("user", t.getUser());
        props.setProperty("password", t.getPassword());
        props.setProperty("loginTimeout",   "300"); // seconds
        props.setProperty("socketTimeout", "1800"); // seconds

        // Enable keepalive based on tcp_keepalive_time, tcp_keepalive_intvl and tcp_keepalive_probes kernel parameters.
        // Socket options TCP_KEEPCNT, TCP_KEEPIDLE, and TCP_KEEPINTVL are not configurable.
        props.setProperty("tcpKeepAlive", "true");

        // TODO
        //switch task.getSssl() {
        //when "disable":
        //    break;
        //when "enable":
        //    props.setProperty("sslfactory", "org.postgresql.ssl.NonValidatingFactory");  // disable server-side validation
        //when "verify":
        //    props.setProperty("ssl", "true");
        //    break;
        //}

        if (!retryableMetadataOperation) {
            // non-retryable batch operation uses longer timeout
            props.setProperty("loginTimeout",    "300");  // seconds
            props.setProperty("socketTimeout", "28800");  // seconds
        }

        props.putAll(t.getOptions());

        return new RedshiftOutputConnector(url, props, t.getSchema());
    }

    @Override
    protected BatchInsert newBatchInsert(PluginTask task, boolean useMerge) throws IOException, SQLException
    {
        if (useMerge) {
            throw new UnsupportedOperationException("Redshift output plugin doesn't support 'merge_direct' mode. Use 'merge' mode instead.");
        }
        RedshiftPluginTask t = (RedshiftPluginTask) task;
        AWSCredentials creds = new BasicAWSCredentials(
                t.getAccessKeyId(), t.getSecretAccessKey());
        return new RedshiftCopyBatchInsert(getConnector(task, true),
                creds, t.getS3Bucket(), t.getIamUserName());
    }
}

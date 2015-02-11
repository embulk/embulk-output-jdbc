package org.embulk.output;

import java.util.Properties;
import java.io.IOException;
import java.sql.SQLException;
import org.slf4j.Logger;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import org.embulk.spi.Exec;
import org.embulk.config.Config;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.redshift.RedshiftOutputConnector;
import org.embulk.output.redshift.RedshiftCopyBatchInsert;

public class RedshiftOutputPlugin
        extends AbstractJdbcOutputPlugin
{
    private static final String DEFAULT_SCHEMA = "public";
    private static final int DEFAULT_PORT = 5439;

    private final Logger logger = Exec.getLogger(RedshiftOutputPlugin.class);

    public interface RedshiftPluginTask extends PluginTask
    {
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
        String url = String.format("jdbc:postgresql://%s:%d/%s",
                task.getHost(), task.getPort().or(DEFAULT_PORT), task.getDatabase());

        Properties props = new Properties();
        props.setProperty("user", task.getUser());
        props.setProperty("password", task.getPassword());
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

        props.putAll(task.getOptions());

        return new RedshiftOutputConnector(url, props, task.getSchema().or(DEFAULT_SCHEMA));
    }

    @Override
    protected BatchInsert newBatchInsert(PluginTask task) throws IOException, SQLException
    {
        RedshiftPluginTask rt = (RedshiftPluginTask) task;
        AWSCredentials creds = new BasicAWSCredentials(
                rt.getAccessKeyId(), rt.getSecretAccessKey());
        return new RedshiftCopyBatchInsert(getConnector(task, true),
                creds, rt.getS3Bucket(), rt.getIamUserName());
    }
}

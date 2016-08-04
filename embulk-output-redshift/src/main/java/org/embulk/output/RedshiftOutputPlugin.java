package org.embulk.output;

import java.util.List;
import java.util.Properties;
import java.io.IOException;
import java.sql.SQLException;

import org.embulk.output.jdbc.MergeConfig;
import org.slf4j.Logger;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import org.embulk.spi.Exec;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.redshift.RedshiftOutputConnector;
import org.embulk.output.redshift.RedshiftCopyBatchInsert;
import org.embulk.output.redshift.Ssl;

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
        @ConfigDefault("\"\"")
        public String getIamUserName();

        @Config("s3_bucket")
        public String getS3Bucket();

        @Config("s3_key_prefix")
        @ConfigDefault("\"\"")
        public String getS3KeyPrefix();

        @Config("ssl")
        @ConfigDefault("disable")
        public Ssl getSsl();
    }

    @Override
    protected Class<? extends PluginTask> getTaskClass()
    {
        return RedshiftPluginTask.class;
    }

    @Override
    protected Features getFeatures(PluginTask task)
    {
        return new Features()
            .setMaxTableNameLength(30)
            .setSupportedModes(ImmutableSet.of(Mode.INSERT, Mode.INSERT_DIRECT, Mode.TRUNCATE_INSERT, Mode.REPLACE, Mode.MERGE))
            .setIgnoreMergeKeys(false);
    }

    @Override
    protected RedshiftOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation)
    {
        RedshiftPluginTask t = (RedshiftPluginTask) task;

        String url = String.format("jdbc:postgresql://%s:%d/%s",
                t.getHost(), t.getPort(), t.getDatabase());

        Properties props = new Properties();
        props.setProperty("loginTimeout",   "300"); // seconds
        props.setProperty("socketTimeout", "1800"); // seconds

        // Enable keepalive based on tcp_keepalive_time, tcp_keepalive_intvl and tcp_keepalive_probes kernel parameters.
        // Socket options TCP_KEEPCNT, TCP_KEEPIDLE, and TCP_KEEPINTVL are not configurable.
        props.setProperty("tcpKeepAlive", "true");

        switch (t.getSsl()) {
        case disable:
           break;
        case enable:
            // See http://docs.aws.amazon.com/redshift/latest/mgmt/connecting-ssl-support.html
           props.setProperty("ssl", "true");
           props.setProperty("sslfactory", "org.postgresql.ssl.NonValidatingFactory"); // disable server-side validation
           break;
        case verify:
           props.setProperty("ssl", "true");
           break;
        }

        if (!retryableMetadataOperation) {
            // non-retryable batch operation uses longer timeout
            props.setProperty("loginTimeout",    "300");  // seconds
            props.setProperty("socketTimeout", "28800");  // seconds
        }

        props.putAll(t.getOptions());

        props.setProperty("user", t.getUser());
        logger.info("Connecting to {} options {}", url, props);
        props.setProperty("password", t.getPassword());

        return new RedshiftOutputConnector(url, props, t.getSchema());
    }

    private static AWSCredentialsProvider getAWSCredentialsProvider(RedshiftPluginTask task)
    {
        final AWSCredentials creds = new BasicAWSCredentials(
                task.getAccessKeyId(), task.getSecretAccessKey());
        return new AWSCredentialsProvider() {
            @Override
            public AWSCredentials getCredentials()
            {
                return creds;
            }

            @Override
            public void refresh()
            {
            }
        };
    }

    @Override
    protected BatchInsert newBatchInsert(PluginTask task, Optional<MergeConfig> mergeConfig) throws IOException, SQLException
    {
        if (mergeConfig.isPresent()) {
            throw new UnsupportedOperationException("Redshift output plugin doesn't support 'merge_direct' mode. Use 'merge' mode instead.");
        }
        RedshiftPluginTask t = (RedshiftPluginTask) task;
        return new RedshiftCopyBatchInsert(getConnector(task, true),
                getAWSCredentialsProvider(t), t.getS3Bucket(), t.getS3KeyPrefix(), t.getIamUserName());
    }
}

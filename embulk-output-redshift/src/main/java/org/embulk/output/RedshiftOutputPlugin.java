package org.embulk.output;

import java.util.Properties;
import java.io.IOException;
import java.sql.SQLException;

import com.google.common.base.Throwables;
import org.embulk.output.jdbc.MergeConfig;
import org.slf4j.Logger;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.amazonaws.auth.AWSCredentialsProvider;
import org.embulk.spi.Exec;
import org.embulk.util.aws.credentials.AwsCredentials;
import org.embulk.util.aws.credentials.AwsCredentialsTaskWithPrefix;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.redshift.RedshiftOutputConnector;
import org.embulk.output.redshift.RedshiftCopyBatchInsert;
import org.embulk.output.redshift.Ssl;
import org.embulk.output.redshift.EncryptOption;

public class RedshiftOutputPlugin
        extends AbstractJdbcOutputPlugin
{
    private final Logger logger = Exec.getLogger(RedshiftOutputPlugin.class);

    public interface RedshiftPluginTask extends AwsCredentialsTaskWithPrefix, PluginTask
    {

        @Config("url")
        public String getUrl();

        @Config("driver_path")
        @ConfigDefault("null")
        public Optional<String> getDriverPath();

        @Config("driver_class")
        public String getDriverClass();

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

        // for backward compatibility
        @Config("access_key_id")
        @ConfigDefault("null")
        Optional<String> getOldAccessKeyId();

        // for backward compatibility
        @Config("secret_access_key")
        @ConfigDefault("null")
        Optional<String> getOldSecretAccessKey();

        @Config("iam_user_name")
        @ConfigDefault("\"\"")
        public String getIamUserName();

        @Config("s3_bucket")
        public String getS3Bucket();
        @Config("s3_region")
        public String getS3Region();

        @Config("s3_key_prefix")
        @ConfigDefault("\"\"")
        public String getS3KeyPrefix();

        @Config("ssl")
        @ConfigDefault("\"disable\"")
        public Ssl getSsl();

        @Config("encrypt_option")
        @ConfigDefault("disable")
        public EncryptOption getEncryptOption();

        @Config("encrypt_key")
        @ConfigDefault("\"\"")
        public String getEncryptKey();
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

        String url = t.getUrl();
        String driverClass = t.getDriverClass();
        if (t.getDriverPath().isPresent()) {
            loadDriverJar(t.getDriverPath().get());
        }
        Properties props = new Properties();
        props.setProperty("loginTimeout",   "300"); // seconds
        props.setProperty("socketTimeout", "1800"); // seconds

        // Enable keepalive based on tcp_keepalive_time, tcp_keepalive_intvl and tcp_keepalive_probes kernel parameters.
        // Socket options TCP_KEEPCNT, TCP_KEEPIDLE, and TCP_KEEPINTVL are not configurable.
        props.setProperty("tcpKeepAlive", "true");

        switch (t.getSsl()) {
        case DISABLE:
           break;
        case ENABLE:
            // See http://docs.aws.amazon.com/redshift/latest/mgmt/connecting-ssl-support.html
           props.setProperty("ssl", "true");
           props.setProperty("sslfactory", "org.postgresql.ssl.NonValidatingFactory"); // disable server-side validation
           break;
        case VERIFY:
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
        return new RedshiftOutputConnector(url, props, t.getSchema(), driverClass);
    }

    private static AWSCredentialsProvider getAWSCredentialsProvider(RedshiftPluginTask task)
    {
        return AwsCredentials.getAWSCredentialsProvider(task);
    }

    private void setAWSCredentialsBackwardCompatibility(RedshiftPluginTask t)
    {
        if ("basic".equals(t.getAuthMethod())) {
            if (t.getOldAccessKeyId().isPresent() && !t.getAccessKeyId().isPresent()) {
                logger.warn("'access_key_id' is deprecated. Please use 'aws_access_key_id'.");
                t.setAccessKeyId(t.getOldAccessKeyId());
            }
            if (t.getOldSecretAccessKey().isPresent() && !t.getSecretAccessKey().isPresent()) {
                logger.warn("'secret_access_key' is deprecated. Please use 'aws_secret_access_key'.");
                t.setSecretAccessKey(t.getOldSecretAccessKey());
            }
        }
    }

    @Override
    protected String generateIntermediateTableNamePrefix(String baseTableName, JdbcOutputConnection con,
            int suffixLength, int maxLength, LengthSemantics lengthSemantics) throws SQLException {
        String namePrefix = super.generateIntermediateTableNamePrefix(baseTableName, con,
                suffixLength, maxLength, lengthSemantics);
        // Table names of Redshift are always lower cases.
        // http://docs.aws.amazon.com/redshift/latest/dg/r_names.html
        // says "identifiers are case-insensitive and are folded to lower case."
        return namePrefix.toLowerCase();
    }

    @Override
    protected BatchInsert newBatchInsert(PluginTask task, Optional<MergeConfig> mergeConfig) throws IOException, SQLException
    {
        if (mergeConfig.isPresent()) {
            throw new UnsupportedOperationException("Redshift output plugin doesn't support 'merge_direct' mode. Use 'merge' mode instead.");
        }
        RedshiftPluginTask t = (RedshiftPluginTask) task;
        setAWSCredentialsBackwardCompatibility(t);
        return new RedshiftCopyBatchInsert(getConnector(task, true),
                getAWSCredentialsProvider(t), t.getS3Bucket(), t.getS3Region(), t.getS3KeyPrefix(), t.getIamUserName(), t.getEncryptOption(), t.getEncryptKey());
        
    }
}

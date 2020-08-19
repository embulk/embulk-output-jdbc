package org.embulk.output;

import java.util.Properties;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.ImmutableSet;
import com.amazonaws.auth.AWSCredentialsProvider;
import org.embulk.util.aws.credentials.AwsCredentials;
import org.embulk.util.aws.credentials.AwsCredentialsTaskWithPrefix;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.output.jdbc.AbstractJdbcOutputPlugin;
import org.embulk.output.jdbc.BatchInsert;
import org.embulk.output.jdbc.JdbcOutputConnection;
import org.embulk.output.jdbc.JdbcOutputConnector;
import org.embulk.output.jdbc.MergeConfig;
import org.embulk.output.jdbc.TableIdentifier;
import org.embulk.output.jdbc.Ssl;
import org.embulk.output.redshift.RedshiftOutputConnector;
import org.embulk.output.redshift.RedshiftCopyBatchInsert;

public class RedshiftOutputPlugin
        extends AbstractJdbcOutputPlugin
{
    private static final Logger logger = LoggerFactory.getLogger(RedshiftOutputPlugin.class);

    public interface RedshiftPluginTask extends AwsCredentialsTaskWithPrefix, PluginTask
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

        @Config("temp_schema")
        @ConfigDefault("null")
        public Optional<String> getTempSchema();

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

        @Config("s3_key_prefix")
        @ConfigDefault("\"\"")
        public String getS3KeyPrefix();

        @Config("delete_s3_temp_file")
        @ConfigDefault("true")
        public boolean getDeleteS3TempFile();

        @Config("ssl")
        @ConfigDefault("\"disable\"")
        public Ssl getSsl();

        @Config("copy_iam_role_name")
        @ConfigDefault("null")
        public Optional<String> getCopyIamRoleName();

        @Config("copy_aws_account_id")
        @ConfigDefault("null")
        public Optional<String> getCopyAwsAccountId();
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
            .setMaxTableNameLength(127)
            .setSupportedModes(ImmutableSet.of(Mode.INSERT, Mode.INSERT_DIRECT, Mode.TRUNCATE_INSERT, Mode.REPLACE, Mode.MERGE))
            .setIgnoreMergeKeys(false);
    }

    @Override
    protected JdbcOutputConnector getConnector(PluginTask task, boolean retryableMetadataOperation)
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
        props.setProperty("password", t.getPassword());
        logConnectionProperties(url, props);

        return new RedshiftOutputConnector(url, props, t.getSchema(), t.getTransactionIsolation());
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
    protected TableIdentifier buildIntermediateTableId(JdbcOutputConnection con, PluginTask task, String tableName) {
        RedshiftPluginTask t = (RedshiftPluginTask) task;
        // replace mode doesn't support temp_schema because ALTER TABLE cannot change schema of table
        if (t.getTempSchema().isPresent() && t.getMode() != Mode.REPLACE) {
            return new TableIdentifier(null, t.getTempSchema().get(), tableName);
        }
        return super.buildIntermediateTableId(con, task, tableName);
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
                getAWSCredentialsProvider(t), t.getS3Bucket(), t.getS3KeyPrefix(), t.getIamUserName(), t.getDeleteS3TempFile(), t.getCopyIamRoleName().orElse(null), t.getCopyAwsAccountId().orElse(null));
    }
}

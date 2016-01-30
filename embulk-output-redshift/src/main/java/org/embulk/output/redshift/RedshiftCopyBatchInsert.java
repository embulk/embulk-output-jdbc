package org.embulk.output.redshift;

import java.util.zip.GZIPOutputStream;
import java.util.concurrent.Callable;
import java.util.UUID;
import java.io.File;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Closeable;
import java.io.BufferedWriter;
import java.sql.SQLException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.Statement.Effect;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.GetFederationTokenRequest;
import com.amazonaws.services.securitytoken.model.GetFederationTokenResult;
import com.amazonaws.services.securitytoken.model.Credentials;
import org.slf4j.Logger;
import org.embulk.spi.Exec;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.postgresql.AbstractPostgreSQLCopyBatchInsert;

public class RedshiftCopyBatchInsert
        extends AbstractPostgreSQLCopyBatchInsert
{
    private final Logger logger = Exec.getLogger(RedshiftCopyBatchInsert.class);
    private final RedshiftOutputConnector connector;
    private final String s3BucketName;
    private final String s3KeyPrefix;
    private final String iamReaderUserName;
    private final AmazonS3Client s3;
    private final AWSSecurityTokenServiceClient sts;
    private final BasicSessionCredentials simple_creds;

    private RedshiftOutputConnection connection = null;
    private String copySqlBeforeFrom = null;
    private long totalRows;
    private int fileCount;

    public static final String COPY_AFTER_FROM = "GZIP DELIMITER '\\t' NULL '\\\\N' ESCAPE TRUNCATECOLUMNS ACCEPTINVCHARS STATUPDATE OFF COMPUPDATE OFF";

    public RedshiftCopyBatchInsert(RedshiftOutputConnector connector,
            AWSCredentialsProvider credentialsProvider, String s3BucketName, String s3KeyPrefix,
            String iamReaderUserName) throws IOException, SQLException
    {
        super();
        this.connector = connector;
        this.s3BucketName = s3BucketName;
        if (s3KeyPrefix.isEmpty() || s3KeyPrefix.endsWith("/")) {
            this.s3KeyPrefix = s3KeyPrefix;
        } else {
            this.s3KeyPrefix = s3KeyPrefix + "/";
        }
        this.iamReaderUserName = iamReaderUserName;
        this.s3 = new AmazonS3Client(credentialsProvider);  // TODO options
        this.sts = new AWSSecurityTokenServiceClient(credentialsProvider);  // options
        this.simple_creds = generateSimpleSessionCredentials(credentialsProvider.getCredentials().getAWSAccessKeyId(), credentialsProvider.getCredentials().getAWSSecretKey());
    }

    @Override
    public void prepare(String loadTable, JdbcSchema insertSchema) throws SQLException
    {
        this.connection = connector.connect(true);
        this.copySqlBeforeFrom = connection.buildCopySQLBeforeFrom(loadTable, insertSchema);
        logger.info("Copy SQL: "+copySqlBeforeFrom+" ? "+COPY_AFTER_FROM);
    }

    @Override
    protected BufferedWriter openWriter(File newFile) throws IOException
    {
        // Redshift supports gzip
        return new BufferedWriter(
                new OutputStreamWriter(
                    new GZIPOutputStream(new FileOutputStream(newFile)),
                    FILE_CHARSET)
                );
    }

    @Override
    public void flush() throws IOException, SQLException
    {
        File file = closeCurrentFile();  // flush buffered data in writer

        // TODO multi-threading
        new UploadAndCopyTask(file, batchRows, s3KeyPrefix + UUID.randomUUID().toString()).call();
        new DeleteFileFinalizer(file).close();

        fileCount++;
        totalRows += batchRows;
        batchRows = 0;

        openNewFile();
        file.delete();
    }

    @Override
    public void finish() throws IOException, SQLException
    {
        super.finish();
        logger.info("Loaded {} files.", fileCount);
    }

    @Override
    public void close() throws IOException, SQLException
    {
        s3.shutdown();
        closeCurrentFile().delete();
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }
    
    private BasicSessionCredentials generateSimpleSessionCredentials(String awsAccessKey, String awsSecretKey) {
    	return new BasicSessionCredentials(awsAccessKey, awsSecretKey, null);
    }

    private BasicSessionCredentials generateReaderSessionCredentials(String s3KeyName)
    {
        Policy policy = new Policy()
            .withStatements(
                    new Statement(Effect.Allow)
                        .withActions(S3Actions.ListObjects)
                        .withResources(new Resource("arn:aws:s3:::"+s3BucketName)),
                    new Statement(Effect.Allow)
                        .withActions(S3Actions.GetObject)
                        .withResources(new Resource("arn:aws:s3:::"+s3BucketName+"/"+s3KeyName))  // TODO encode file name using percent encoding
                    );
        GetFederationTokenRequest req = new GetFederationTokenRequest();
        req.setDurationSeconds(86400);  // 3600 - 129600
        req.setName(iamReaderUserName);
        req.setPolicy(policy.toJson());

        GetFederationTokenResult res = sts.getFederationToken(req);
        Credentials c = res.getCredentials();

        return new BasicSessionCredentials(
                c.getAccessKeyId(),
                c.getSecretAccessKey(),
                c.getSessionToken());
    }

    private class UploadAndCopyTask implements Callable<Void>
    {
        private final File file;
        private final int batchRows;
        private final String s3KeyName;

        public UploadAndCopyTask(File file, int batchRows, String s3KeyName)
        {
            this.file = file;
            this.batchRows = batchRows;
            this.s3KeyName = s3KeyName;
        }

        public Void call() throws SQLException {
            logger.info(String.format("Uploading file id %s to S3 (%,d bytes %,d rows)",
                        s3KeyName, file.length(), batchRows));
            s3.putObject(s3BucketName, s3KeyName, file);

            RedshiftOutputConnection con = connector.connect(true);
            try {
                logger.info("Running COPY from file {}", s3KeyName);

                // create temporary credential right before COPY operation because
                // it has timeout.
                BasicSessionCredentials creds = null;
                if (iamReaderUserName != null && iamReaderUserName.length() > 0)
                	creds = generateReaderSessionCredentials(s3KeyName);
                else
                	creds = simple_creds;

                long startTime = System.currentTimeMillis();
                con.runCopy(buildCopySQL(creds));
                double seconds = (System.currentTimeMillis() - startTime) / 1000.0;

                logger.info(String.format("Loaded file %s (%.2f seconds for COPY)", s3KeyName, seconds));

            } finally {
                con.close();
            }

            return null;
        }

        private String buildCopySQL(BasicSessionCredentials creds)
        {
            StringBuilder sb = new StringBuilder();
            sb.append(copySqlBeforeFrom);
            sb.append(" FROM 's3://");
            sb.append(s3BucketName);
            sb.append("/");
            sb.append(s3KeyName);
            sb.append("' CREDENTIALS '");
            sb.append("aws_access_key_id=");
            sb.append(creds.getAWSAccessKeyId());
            sb.append(";aws_secret_access_key=");
            sb.append(creds.getAWSSecretKey());
            if (creds.getSessionToken() != null) {
                sb.append(";token=");
                sb.append(creds.getSessionToken());
            }
            sb.append("' ");
            sb.append(COPY_AFTER_FROM);
            return sb.toString();
        }
    }

    private static class DeleteFileFinalizer implements Closeable
    {
        private File file;

        public DeleteFileFinalizer(File file) {
            this.file = file;
        }

        @Override
        public void close() throws IOException {
            file.delete();
        }
    }
}

package org.embulk.output.redshift;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.postgresql.AbstractPostgreSQLCopyBatchInsert;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.Statement.Effect;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetFederationTokenRequest;
import com.amazonaws.services.securitytoken.model.GetFederationTokenResult;

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
    private final ExecutorService executorService;

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

        this.executorService = Executors.newCachedThreadPool();
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

        UploadAndCopyTask task = new UploadAndCopyTask(file, batchRows, s3KeyPrefix + UUID.randomUUID().toString());
        executorService.submit(task);

        fileCount++;
        totalRows += batchRows;
        batchRows = 0;

        openNewFile();
    }

    @Override
    public void finish() throws IOException, SQLException
    {
        super.finish();

        executorService.shutdown();
        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        logger.info("Loaded {} files.", fileCount);
    }

    @Override
    public void close() throws IOException, SQLException
    {
        if (!executorService.isShutdown()) {
            executorService.shutdown();
        }
        s3.shutdown();
        closeCurrentFile().delete();
        if (connection != null) {
            connection.close();
            connection = null;
        }
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
                // TODO skip this step if iamReaderUserName is not set
                BasicSessionCredentials creds = generateReaderSessionCredentials(s3KeyName);

                long startTime = System.currentTimeMillis();
                con.runCopy(buildCopySQL(creds));
                double seconds = (System.currentTimeMillis() - startTime) / 1000.0;

                logger.info(String.format("Loaded file %s (%.2f seconds for COPY)", s3KeyName, seconds));

            } finally {
                try {
                    con.close();
                } finally {
                    file.delete();
                    s3.deleteObject(s3BucketName, s3KeyName);
                }
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
            sb.append(";token=");
            sb.append(creds.getSessionToken());
            sb.append("' ");
            sb.append(COPY_AFTER_FROM);
            return sb.toString();
        }
    }

}

package org.embulk.output.redshift;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import org.embulk.output.jdbc.JdbcOutputConnector;
import org.embulk.output.jdbc.JdbcSchema;
import org.embulk.output.jdbc.TableIdentifier;
import org.embulk.output.postgresql.AbstractPostgreSQLCopyBatchInsert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.auth.policy.Resource;
import com.amazonaws.auth.policy.Statement;
import com.amazonaws.auth.policy.Statement.Effect;
import com.amazonaws.auth.policy.actions.S3Actions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest;
import com.amazonaws.services.securitytoken.model.GetCallerIdentityResult;
import com.amazonaws.services.securitytoken.model.GetFederationTokenRequest;
import com.amazonaws.services.securitytoken.model.GetFederationTokenResult;

public class RedshiftCopyBatchInsert
        extends AbstractPostgreSQLCopyBatchInsert
{
    private static final Logger logger = LoggerFactory.getLogger(RedshiftCopyBatchInsert.class);
    private final JdbcOutputConnector connector;
    private final String s3BucketName;
    private final String s3KeyPrefix;
    private final String iamReaderUserName;
    private final boolean deleteS3TempFile;
    private final AWSCredentialsProvider credentialsProvider;
    private final AmazonS3Client s3;
    private final String s3RegionName;
    private final AWSSecurityTokenServiceClient sts;
    private final ExecutorService executorService;
    private final String copyIamRoleARN;

    private RedshiftOutputConnection connection = null;
    private String copySqlBeforeFrom = null;
    private long totalRows;
    private int fileCount;
    private List<Future<Void>> uploadAndCopyFutures;

    public static final String COPY_AFTER_FROM = "GZIP DELIMITER '\\t' NULL '\\\\N' ESCAPE TRUNCATECOLUMNS ACCEPTINVCHARS STATUPDATE OFF COMPUPDATE OFF";

    public RedshiftCopyBatchInsert(JdbcOutputConnector connector,
            AWSCredentialsProvider credentialsProvider, String s3BucketName, String s3KeyPrefix,
            String iamReaderUserName, boolean deleteS3TempFile, String copyIamRoleName, String copyAwsAccountId) throws IOException, SQLException
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
        this.deleteS3TempFile = deleteS3TempFile;
        this.credentialsProvider = credentialsProvider;
        this.s3 = new AmazonS3Client(credentialsProvider);  // TODO options
        this.sts = new AWSSecurityTokenServiceClient(credentialsProvider);  // options
        this.executorService = Executors.newCachedThreadPool();
        this.uploadAndCopyFutures = new ArrayList<Future<Void>>();

        String s3RegionName = null;
        try {
            String s3Location = s3.getBucketLocation(s3BucketName);
            Region s3Region = Region.fromValue(s3Location);
            com.amazonaws.regions.Region region = s3Region.toAWSRegion();
            s3RegionName = region.getName();
            logger.info("S3 region for bucket '" + s3BucketName + "' is '" + s3RegionName + "'.");
        } catch (AmazonClientException | IllegalArgumentException e) {
            logger.warn("Cannot get S3 region for bucket '" + s3BucketName + "'."
                    + " IAM user needs \"s3:GetBucketLocation\" permission if Redshift region and S3 region are different.");
        }
        this.s3RegionName = s3RegionName;

        if (copyIamRoleName != null) {
            String accountId = null;
            if (copyAwsAccountId != null) {
                accountId = copyAwsAccountId;
            } else {
                GetCallerIdentityRequest request = new GetCallerIdentityRequest();
                GetCallerIdentityResult response = this.sts.getCallerIdentity(request);
                accountId = response.getAccount();
            }
            this.copyIamRoleARN = "arn:aws:iam::" + accountId + ":role/" + copyIamRoleName;
        } else {
            this.copyIamRoleARN = null;
        }
    }

    @Override
    public void prepare(TableIdentifier loadTable, JdbcSchema insertSchema) throws SQLException
    {
        this.connection = (RedshiftOutputConnection)connector.connect(true);
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

        String s3KeyName = s3KeyPrefix + UUID.randomUUID().toString();
        UploadTask uploadTask = new UploadTask(file, batchRows, s3KeyName);
        Future<Void> uploadFuture = executorService.submit(uploadTask);
        uploadAndCopyFutures.add(uploadFuture);

        CopyTask copyTask = new CopyTask(uploadFuture, s3KeyName);
        uploadAndCopyFutures.add(executorService.submit(copyTask));

        fileCount++;
        totalRows += batchRows;
        batchRows = 0;

        openNewFile();
    }

    @Override
    public void finish() throws IOException, SQLException
    {
        for (Future<Void> uploadAndCopyFuture : uploadAndCopyFutures) {
            try {
                uploadAndCopyFuture.get();

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof SQLException) {
                    throw (SQLException)e.getCause();
                }
                throw new RuntimeException(e);
            }
        }

        logger.info("Loaded {} files.", fileCount);
    }

    @Override
    public void close() throws IOException, SQLException
    {
        executorService.shutdownNow();
        try {
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {}

        s3.shutdown();
        closeCurrentFile().delete();
        if (connection != null) {
            connection.close();
            connection = null;
        }
    }

    // Add \ before \, \n, \t
    // Remove \0
    @Override
    protected String escape(char c)
    {
        switch (c) {
        case '\n':
            return "\\\n";
        case '\t':
            return "\\\t";
        case '\r':
            return String.valueOf(c);
        default:
            return super.escape(c);
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
        if (iamReaderUserName != null && iamReaderUserName.length() > 0) {
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
        } else {
            if (credentialsProvider.getCredentials() instanceof BasicSessionCredentials) {
                BasicSessionCredentials credentials = (BasicSessionCredentials) credentialsProvider.getCredentials();
                return credentials;
            }
            return new BasicSessionCredentials(credentialsProvider.getCredentials().getAWSAccessKeyId(),
                    credentialsProvider.getCredentials().getAWSSecretKey(), null);
        }
    }

    private class UploadTask implements Callable<Void>
    {
        private final File file;
        private final int batchRows;
        private final String s3KeyName;

        public UploadTask(File file, int batchRows, String s3KeyName)
        {
            this.file = file;
            this.batchRows = batchRows;
            this.s3KeyName = s3KeyName;
        }

        public Void call() {
            logger.info(String.format("Uploading file id %s to S3 (%,d bytes %,d rows)",
                        s3KeyName, file.length(), batchRows));

            try {
                long startTime = System.currentTimeMillis();
                s3.putObject(s3BucketName, s3KeyName, file);
                double seconds = (System.currentTimeMillis() - startTime) / 1000.0;

                logger.info(String.format("Uploaded file %s (%.2f seconds)", s3KeyName, seconds));
            } finally {
                file.delete();
            }

            return null;
        }
    }

    private class CopyTask implements Callable<Void>
    {
        private final Future<Void> uploadFuture;
        private final String s3KeyName;

        public CopyTask(Future<Void> uploadFuture, String s3KeyName)
        {
            this.uploadFuture = uploadFuture;
            this.s3KeyName = s3KeyName;
        }

        public Void call() throws SQLException, InterruptedException, ExecutionException {
            try {
                uploadFuture.get();

                RedshiftOutputConnection con = (RedshiftOutputConnection)connector.connect(true);
                try {
                    logger.info("Running COPY from file {}", s3KeyName);

                    // create temporary credential right before COPY operation because
                    // it has timeout.
                    BasicSessionCredentials creds = generateReaderSessionCredentials(s3KeyName);

                    long startTime = System.currentTimeMillis();
                    con.runCopy(buildCopySQL(creds));
                    double seconds = (System.currentTimeMillis() - startTime) / 1000.0;

                    logger.info(String.format("Loaded file %s (%.2f seconds for COPY)", s3KeyName, seconds));

                } finally {
                    con.close();
                }
            } finally {
                if (deleteS3TempFile) {
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
            if (copyIamRoleARN != null) {
                sb.append("aws_iam_role=");
                sb.append(copyIamRoleARN);
            } else {
                sb.append("aws_access_key_id=");
                sb.append(creds.getAWSAccessKeyId());
                sb.append(";aws_secret_access_key=");
                sb.append(creds.getAWSSecretKey());
                if (creds.getSessionToken() != null) {
                    sb.append(";token=");
                    sb.append(creds.getSessionToken());
                }
            }
            sb.append("' ");
            if (s3RegionName != null) {
                sb.append("REGION '");
                sb.append(s3RegionName);
                sb.append("' ");
            }

            sb.append(COPY_AFTER_FROM);
            return sb.toString();
        }
    }
}

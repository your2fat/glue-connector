package com.amazonaws.athena.connectors.elasticsearch;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.amazonaws.services.glue.model.GetJobRequest;
import com.amazonaws.services.glue.model.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobParameters {

    private static final Logger logger = LoggerFactory.getLogger(JobParameters.class);

    private AWSGlue awsGlue;

    private Job currentJob;

    public JobParameters() {
        awsGlue = AWSGlueClientBuilder.defaultClient();
    }

    private Job getCurrentJob() {
        if (currentJob == null) {
            logger.info("env: {}", System.getenv());
            logger.info("job name: {}", System.getenv("JOB_NAME"));
            GetJobRequest getJobRequest = new GetJobRequest().withJobName(System.getenv("JOB_NAME"));
            logger.info("getJobRequest: {}", getJobRequest);
            currentJob = awsGlue.getJob(getJobRequest).getJob();
        }
        return currentJob;
    }

    public String get(String key) {
        return getCurrentJob().getDefaultArguments().get(key);
    }

}

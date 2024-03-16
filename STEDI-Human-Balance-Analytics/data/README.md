# Purpose of this Folder

This folder should contain public project starter code.

Upload the directories to your S3 bucket and use them to create your Glue tables.

When you have completed this project, your S3 bucket should have the following directory structure:

```
customer/
- landing/
- trusted/
- curated/
accelerometer/
- landing/
- trusted/
step_trainer/
- landing/
- trusted/
- curated/
```

**Note:** `step_trainer/curated/` contains the data files for the `machine_learning_curated` table.

aws iam put-role-policy --role-name my-glue-service-role --policy-name S3Access --policy-document '{ "Version": "2012-10-17", "Statement": [ { "Sid": "ListObjectsInBucket", "Effect": "Allow", "Action": [ "s3:ListBucket" ], "Resource": [ "arn:aws:s3:::pj3oriental" ] }, { "Sid": "AllObjectActions", "Effect": "Allow", "Action": "s3:*Object", "Resource": [ "arn:aws:s3:::pj3oriental/*" ] } ] }'

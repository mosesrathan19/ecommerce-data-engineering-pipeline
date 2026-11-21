# Phase 5 — AWS S3, Databricks, Delta Lake and Unity Catalog

**This is a deployment-ready educational example, not an automatically provisioned cloud environment.**

## Free local/Colab learning
You already have Phases 1–3 and the code to run without AWS costs. To explore Databricks without connecting your AWS bucket, use an eligible Databricks workspace with Unity Catalog volume support and upload the 22 synthetic raw CSV files into a volume.

## AWS + Databricks path (requires accounts and permissions; may incur charges)
1. Create or select a private S3 bucket in your AWS account, in a region accessible to your Databricks workspace. Enable block public access, encryption and appropriate retention/lifecycle policies.
2. Configure AWS CLI credentials **outside GitHub**. Check `aws sts get-caller-identity`.
3. From repo root: `export ECOMMERCE_S3_BUCKET=YOUR_BUCKET`; `bash cloud/aws/upload_source_to_s3.sh`. This uploads the CSVs to `s3://YOUR_BUCKET/ecommerce/raw/`.
4. In Databricks on AWS, use **Catalog Explorer / Unity Catalog** to create a storage credential and external location backed by an IAM role with least-privilege access to the target S3 prefix. Do not paste static access keys into notebooks.
5. Create an **external volume** over the raw-data prefix, or use a managed volume and upload files. Grant the notebook principal `READ VOLUME`, `USE CATALOG`, `USE SCHEMA`, and table creation/write privileges as required.
6. Import `notebooks/05_databricks_delta_pipeline.py` into a Databricks workspace as a **source notebook**, choose a catalog/schema/volume you can access, and run its cells.
7. Review generated managed Delta tables and MERGE output. Optionally schedule the notebook using **Databricks Workflows** / Lakeflow Jobs after testing.

**Paths:** The notebook reads from `/Volumes/<catalog>/<schema>/<volume>/raw/`; upload CSVs into that directory. If your external volume is already mounted at the raw prefix root, adjust the notebook's `RAW_DIR` accordingly.

**Expected behavior:** 100,100 raw rows → 99,375 clean orders (after dedup/validation) → 99,865 after a simulated CDC MERGE (500 inserts, 40 updates, 10 deletes). Existing tables are not replaced on a rerun, so the example demonstrates repeatable MERGE effects. This is not real log-based CDC, and it does not maintain source offsets or persist per-key sequence numbers.

**Storage:** Tables use Databricks-managed Delta storage in the chosen Unity Catalog schema. Input raw files can originate from an AWS S3-backed external volume, but the example does *not* directly create AWS resources, access keys or IAM roles.

**Security & cost:** Set an AWS budget/alarm, limit IAM rights, do not expose S3 buckets publicly, do not commit credentials, and delete unused cloud resources after practice.

Official docs:
- https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3
- https://docs.databricks.com/aws/en/volumes
- https://docs.databricks.com/aws/en/getting-started/free-edition-limitations

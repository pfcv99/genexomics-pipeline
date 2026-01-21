# Cost Estimation

This document provides the cost estimation for the GeneXOmics bioinformatics pipeline using the **AWS Pricing Calculator**.

## AWS Pricing Calculator Estimate

The following estimate was created using the AWS Pricing Calculator and reflects the assumptions described in the project documentation (data volume, compute model, storage, and processing frequency):

🔗 **AWS Pricing Calculator link**  
https://calculator.aws/#/estimate?id=9d1c21f53892ca597ba361717234e5cf74f6b307

## Scope of the Estimate

The estimate includes:

- **Amazon S3**
  - Storage for sequencing data (~1.1 TB/month)
  - Standard storage class
  - Typical PUT/GET request volumes
  - Limited data transfer out

- **Compute (AWS Batch on EC2)**
  - CPU-based EC2 instances for pipeline execution
  - Assumed execution time per sequencing run
  - Monthly aggregation based on ~32 runs/month
  - On-Demand pricing used as baseline (Spot Instances can significantly reduce costs)

- **EBS (gp3)**
  - Temporary scratch storage attached to compute instances
  - Pro-rated usage based on job duration

## Exclusions

The following costs are **not included** in this estimate:

- Quilt licensing or managed Quilt services
- Long-term archival storage (e.g. Glacier)
- Savings Plans or Reserved Instances
- Extended monitoring and logging costs
- Data transfer beyond basic egress assumptions

## Notes

- This estimate is intended as a **baseline** to understand order-of-magnitude costs.
- Actual costs may vary depending on:
  - Pipeline runtime
  - Instance types
  - Spot instance availability
  - Data access patterns
- The estimate can be duplicated and adjusted directly in the AWS Pricing Calculator to explore alternative scenarios.

---
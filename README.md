# STEDI Human Balance Analytics – AWS Lakehouse Project

## Project Overview
This project builds a data lakehouse solution on AWS for the STEDI Step Trainer system.
The goal is to curate sensor and mobile application data so that it can be safely used
by Data Scientists to train a machine learning model for step detection, while respecting
customer privacy and consent requirements.

## Data Architecture
The lakehouse follows a three-zone architecture:

- **Landing Zone**: Raw JSON data ingested from S3
- **Trusted Zone**: Privacy-filtered and validated data
- **Curated Zone**: Analytics- and machine-learning-ready datasets

## AWS Services Used
- Amazon S3
- AWS Glue (Glue Studio, Spark)
- AWS Athena
- Python (PySpark)

## Glue Jobs Summary
| Glue Job | Description |
|--------|-------------|
| customer_landing_to_trusted | Filters customers who consented to research |
| accelerometer_landing_to_trusted | Filters accelerometer data using trusted customers |
| customer_trusted_to_curated | Selects customers with accelerometer data |
| step_trainer_trusted | Filters step trainer data using curated customers |
| machine_learning_curated | Creates final ML training dataset |

## Row Count Validation
| Zone | Table | Row Count |
|-----|-------|-----------|
| Landing | customer_landing | 956 |
| Landing | accelerometer_landing | 81273 |
| Landing | step_trainer_landing | 28680 |
| Trusted | customer_trusted | 482 |
| Trusted | accelerometer_trusted | 40981 |
| Trusted | step_trainer_trusted | 14460 |
| Curated | customers_curated | 482 |
| Curated | machine_learning_curated | 43681 |

## Notes
- Glue Studio SQL Query transform nodes were used to ensure consistent joins.


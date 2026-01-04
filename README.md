# Adventure Works — Sales & Analytics

This repository contains an end-to-end Analytics Engineering project built for the fictional company Adventure Works.
The objective of the project is to transform raw transactional data (OLTP) into a trusted, scalable data model, enabling business users and executives to explore sales performance through a structured and governed semantic layer.
This project was developed following industry best practices for dimensional modeling, data transformation, data quality, and analytics consumption.

## Project Stack:

- Databricks: Data Warehouse / Lakehouse
- dbt: Data transformation, testing, and documentation
- Power BI:Analytics & Visualization
- GitHub: Version Control

## Project Structure:

```pgsql
models/
├── staging/
├── intermediate/
└── marts/
    ├── dim__customers.sql
    ├── dim__products.sql
    ├── dim__dates.sql
    ├── dim__sales_reason.sql
    ├── dim__location.sql
    └── fct__sales.sql

```

- Staging: in this layer, raw tables were refined, getting only the needed columns, casting data types and renaming columns.
- Intermediate: in this layer, tables were joined and some basic logic applied.
- Marts: in this layer, we have our final tables with the business logic applied.

## Dimensional Modeling:

Below you can access the visualizations:

[Original ERD](https://drive.google.com/file/d/14zQMR68W884BrbHsitqVBdm49kyJMayp/view?usp=sharing): Adventure Works OLTP schema

[Conceptual Model](https://drive.google.com/file/d/1AvuyObcwPKZ83xD_8k0Rtra5GjSK-mrV/view?usp=sharing): Overview of the entities used to build the final models

[Star Schema](https://drive.google.com/file/d/1ddnXvJ3TqGuZ9Snv8-7jSIgwR63lzcK1/view?usp=sharing): The dimensional model of the project, with the fact and dimension tables

[dbt lineage](https://drive.google.com/file/d/18IlqztL5a6LTG5J5o9XpffELd_gmOmhX/view?usp=sharing): The dag (Directed Acyclic Graph) of the project, ilustrating how the dependencies of the models

## Analytics & Dashboard

The dashboard is designed for both executive and analytical users.
Here you can access the [dashboard](https://app.powerbi.com/view?r=eyJrIjoiZDk1NWIxOWYtMWFiNC00MzNlLTllZWYtM2RlNmE1MTRmMmJmIiwidCI6IjI0NzU1NTI2LWE5YWEtNGEwMS04ZjFkLWRlNDk1NjEzYjE3YSJ9).

Dashboard Pages
Menu: Central navigation hub
Executive Overview: High-level KPIs and trends
Sales Performance: Product performance and ticket analysis
Customers: Top customers and regional insights
Products: Information about products and categories

All visuals consume data exclusively from the dbt-built marts models.
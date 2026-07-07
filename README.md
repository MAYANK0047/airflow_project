<div align="center">

  <h1>⚙️ Enterprise ETL CI/CD Pipeline</h1>
  <p><i>Automated GitOps Architecture for Databricks Serverless SQL</i></p>

  <!-- Tech Stack Badges -->
  <img src="https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white" alt="Databricks" />
  <img src="https://img.shields.io/badge/GitHub_Actions-2088FF?style=for-the-badge&logo=githubactions&logoColor=white" alt="GitHub Actions" />
  <img src="https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" alt="Airflow" />
  <img src="https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white" alt="SQL" />

</div>

<br>
<hr>

<h2>📌 Overview</h2>
<p>
  This repository houses a fully automated, GitOps-driven ETL pipeline. It establishes a robust CI/CD workflow that tracks, versions, and automatically deploys database configurations and metadata changes directly to a <b>Databricks Serverless SQL Warehouse</b>.
</p>
<p>
  By replacing manual SQL execution with an automated deployment pipeline, this project ensures a complete, auditable history for all data warehouse modifications (such as dynamically updating SCD tracking strategies for dimensional modeling).
</p>

<br>

<h2>🏗️ Architecture & Tools</h2>
<table>
  <tr>
    <td width="20%" align="center">☁️ <b>Databricks</b></td>
    <td>Serverless SQL Warehouse providing instant, scalable compute for executing pure SQL workloads.</td>
  </tr>
  <tr>
    <td width="20%" align="center">🔄 <b>GitHub Actions</b></td>
    <td>CI/CD engine that automatically authenticates and pushes code changes to the Databricks environment.</td>
  </tr>
  <tr>
    <td width="20%" align="center">💻 <b>Local Dev</b></td>
    <td>VS Code & SQLTools configured for direct, secure querying of the Databricks catalog without heavy Python environment overhead.</td>
  </tr>
</table>

<br>

<h2>📂 Project Structure</h2>

```text
airflow_project/
├── .github/
│   └── workflows/
│       └── deploy.yml          # CI/CD pipeline configuration
├── metadata_admin/
│   └── change_scd_strategy.sql # Version-controlled SQL scripts 
├── .gitignore                  # Prevents local SQL scratchpads from being tracked
└── README.md

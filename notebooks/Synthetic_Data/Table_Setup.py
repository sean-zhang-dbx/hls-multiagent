# Databricks notebook source
# DBTITLE 1,cleanup
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- DROP STATEMENTS - BIMAL DATA DISCOVERY CATALOG
# MAGIC -- WARNING: This will delete all data permanently!
# MAGIC -- ============================================================================
# MAGIC
# MAGIC USE CATALOG sean_zhang_catalog;
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- DROP TABLES - CLINICAL RESEARCH SCHEMA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_clinical_research.adverse_events;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_clinical_research.real_world_evidence;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_clinical_research.genomics_datasets;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_clinical_research.clinical_endpoints;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_clinical_research.patient_cohorts;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_clinical_research.clinical_trials;
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- DROP TABLES - REGULATORY COMPLIANCE SCHEMA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_regulatory_compliance.pharmacovigilance_reports;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_regulatory_compliance.retention_schedules;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_regulatory_compliance.audit_trail_logs;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_regulatory_compliance.gxp_validated_systems;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_regulatory_compliance.regulatory_submissions;
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- DROP TABLES - PATIENT PRIVACY SCHEMA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_patient_privacy.special_category_data;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_patient_privacy.anonymization_metadata;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_patient_privacy.data_subject_requests;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_patient_privacy.consent_records;
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- DROP TABLES - MANUFACTURING OPERATIONS SCHEMA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_manufacturing_ops.process_deviations;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_manufacturing_ops.supply_chain_inventory;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_manufacturing_ops.manufacturing_sites;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_manufacturing_ops.quality_control_results;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_manufacturing_ops.batch_genealogy;
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- DROP TABLES - COMMERCIAL ANALYTICS SCHEMA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_commercial_analytics.competitive_intelligence;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_commercial_analytics.hcp_engagement;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_commercial_analytics.market_access_contracts;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_commercial_analytics.prescription_trends;
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- DROP TABLES - INFRASTRUCTURE SCHEMA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_infrastructure.integration_interfaces;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_infrastructure.data_quality_metrics;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_infrastructure.data_pipelines;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_infrastructure.source_systems_catalog;
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- DROP TABLES - GOVERNANCE SCHEMA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_governance.governance_policies;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_governance.data_classification;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_governance.data_ownership_registry;
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- DROP TABLES - AI/ML ANALYTICS SCHEMA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_ai_ml_analytics.ml_training_datasets;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_ai_ml_analytics.feature_stores;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_ai_ml_analytics.ml_models_registry;
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- DROP TABLES - REFERENCE DATA SCHEMA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_reference_data.controlled_vocabularies;
# MAGIC DROP TABLE IF EXISTS sean_zhang_catalog.hls_reference_data.therapeutic_areas;
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- DROP SCHEMAS
# MAGIC -- ============================================================================
# MAGIC
# MAGIC DROP SCHEMA IF EXISTS sean_zhang_catalog.hls_clinical_research CASCADE;
# MAGIC DROP SCHEMA IF EXISTS sean_zhang_catalog.hls_regulatory_compliance CASCADE;
# MAGIC DROP SCHEMA IF EXISTS sean_zhang_catalog.hls_patient_privacy CASCADE;
# MAGIC DROP SCHEMA IF EXISTS sean_zhang_catalog.hls_manufacturing_ops CASCADE;
# MAGIC DROP SCHEMA IF EXISTS sean_zhang_catalog.hls_commercial_analytics CASCADE;
# MAGIC DROP SCHEMA IF EXISTS sean_zhang_catalog.hls_infrastructure CASCADE;
# MAGIC DROP SCHEMA IF EXISTS sean_zhang_catalog.hls_governance CASCADE;
# MAGIC DROP SCHEMA IF EXISTS sean_zhang_catalog.hls_ai_ml_analytics CASCADE;
# MAGIC DROP SCHEMA IF EXISTS sean_zhang_catalog.hls_reference_data CASCADE;
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- OPTIONAL: DROP CATALOG (Uncomment if you want to drop the entire catalog)
# MAGIC -- ============================================================================
# MAGIC
# MAGIC -- DROP CATALOG IF EXISTS hls_data_discovery CASCADE;
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- COMPLETION MESSAGE
# MAGIC -- ============================================================================
# MAGIC
# MAGIC SELECT 'Bimal Data Discovery - All tables and schemas dropped!' AS status,
# MAGIC        '32 tables dropped' AS tables_dropped,
# MAGIC        '9 schemas dropped' AS schemas_dropped;
# MAGIC

# COMMAND ----------

# DBTITLE 1,schema and table creation
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- PART 1: CATALOG AND SCHEMA CREATION
# MAGIC -- ============================================================================
# MAGIC -- ============================================================================
# MAGIC -- Catalog: HLS_Data_Discovery
# MAGIC -- ============================================================================
# MAGIC -- Using existing catalog sean_zhang_catalog (no CREATE needed on FEVM)
# MAGIC
# MAGIC
# MAGIC -- Use the catalog
# MAGIC USE CATALOG sean_zhang_catalog;
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- Schema 1: Clinical Research
# MAGIC -- ============================================================================
# MAGIC CREATE SCHEMA IF NOT EXISTS hls_clinical_research
# MAGIC COMMENT 'Clinical trial and R&D research data assets';
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- Schema 2: Regulatory Compliance
# MAGIC -- ============================================================================
# MAGIC CREATE SCHEMA IF NOT EXISTS hls_regulatory_compliance
# MAGIC COMMENT 'Regulatory submissions, compliance, and pharmacovigilance data';
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- Schema 3: Patient Privacy
# MAGIC -- ============================================================================
# MAGIC CREATE SCHEMA IF NOT EXISTS hls_patient_privacy
# MAGIC COMMENT 'Patient consent, privacy controls, and data subject rights management';
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- Schema 4: Manufacturing Operations
# MAGIC -- ============================================================================
# MAGIC CREATE SCHEMA IF NOT EXISTS hls_manufacturing_ops
# MAGIC COMMENT 'Manufacturing, quality control, and supply chain data';
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- Schema 5: Commercial Analytics
# MAGIC -- ============================================================================
# MAGIC CREATE SCHEMA IF NOT EXISTS hls_commercial_analytics
# MAGIC COMMENT 'Commercial, market access, and competitive intelligence data';
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- Schema 6: Infrastructure
# MAGIC -- ============================================================================
# MAGIC CREATE SCHEMA IF NOT EXISTS hls_infrastructure
# MAGIC COMMENT 'Technical systems, data pipelines, and integration metadata';
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- Schema 7: Governance
# MAGIC -- ============================================================================
# MAGIC CREATE SCHEMA IF NOT EXISTS hls_governance
# MAGIC COMMENT 'Data ownership, stewardship, and governance policies';
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- Schema 8: AI/ML Analytics
# MAGIC -- ============================================================================
# MAGIC CREATE SCHEMA IF NOT EXISTS hls_ai_ml_analytics
# MAGIC COMMENT 'Machine learning models, feature stores, and training datasets';
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- Schema 9: Reference Data
# MAGIC -- ============================================================================
# MAGIC CREATE SCHEMA IF NOT EXISTS hls_reference_data
# MAGIC COMMENT 'Master data and controlled vocabularies';
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- PART 2: TABLE CREATION - CLINICAL RESEARCH SCHEMA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC USE SCHEMA hls_clinical_research;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS clinical_trials (
# MAGIC     trial_id STRING NOT NULL,
# MAGIC     trial_name STRING,
# MAGIC     protocol_number STRING,
# MAGIC     therapeutic_area STRING,
# MAGIC     study_phase STRING,
# MAGIC     molecule_code STRING,
# MAGIC     primary_endpoint STRING,
# MAGIC     enrollment_target INT,
# MAGIC     enrollment_actual INT,
# MAGIC     study_start_date DATE,
# MAGIC     study_end_date DATE,
# MAGIC     principal_investigator STRING,
# MAGIC     sponsor STRING,
# MAGIC     trial_status STRING,
# MAGIC     clinicaltrials_gov_id STRING,
# MAGIC     data_owner_email STRING,
# MAGIC     business_steward_email STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Master clinical trial registry with protocol and enrollment information';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS patient_cohorts (
# MAGIC     cohort_id STRING NOT NULL,
# MAGIC     trial_id STRING,
# MAGIC     cohort_name STRING,
# MAGIC     patient_count INT,
# MAGIC     inclusion_criteria STRING,
# MAGIC     exclusion_criteria STRING,
# MAGIC     geographic_origin STRING,
# MAGIC     age_range_min INT,
# MAGIC     age_range_max INT,
# MAGIC     gender_distribution STRING,
# MAGIC     ethnicity_distribution STRING,
# MAGIC     consent_type STRING,
# MAGIC     identifiability_level STRING,
# MAGIC     special_population_flag BOOLEAN,
# MAGIC     data_sensitivity STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Patient cohort definitions and demographics for clinical trials';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS clinical_endpoints (
# MAGIC     endpoint_id STRING NOT NULL,
# MAGIC     trial_id STRING,
# MAGIC     endpoint_name STRING,
# MAGIC     endpoint_type STRING,
# MAGIC     measurement_unit STRING,
# MAGIC     measurement_method STRING,
# MAGIC     data_collection_frequency STRING,
# MAGIC     target_value DOUBLE,
# MAGIC     statistical_method STRING,
# MAGIC     endpoint_status STRING,
# MAGIC     data_modality STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Clinical trial endpoints and measurement specifications';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS genomics_datasets (
# MAGIC     dataset_id STRING NOT NULL,
# MAGIC     dataset_name STRING,
# MAGIC     trial_id STRING,
# MAGIC     sequencing_platform STRING,
# MAGIC     data_modality STRING,
# MAGIC     genome_build STRING,
# MAGIC     sample_count INT,
# MAGIC     sequencing_depth STRING,
# MAGIC     variant_count BIGINT,
# MAGIC     file_format STRING,
# MAGIC     storage_path STRING,
# MAGIC     data_size_gb DOUBLE,
# MAGIC     consent_scope STRING,
# MAGIC     identifiability_level STRING,
# MAGIC     analysis_ready BOOLEAN,
# MAGIC     ml_training_approved BOOLEAN,
# MAGIC     data_owner_email STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Genomic sequencing datasets from clinical and research programs';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS real_world_evidence (
# MAGIC     rwe_id STRING NOT NULL,
# MAGIC     dataset_name STRING,
# MAGIC     data_source STRING,
# MAGIC     therapeutic_area STRING,
# MAGIC     patient_population STRING,
# MAGIC     data_type STRING,
# MAGIC     geographic_coverage STRING,
# MAGIC     time_period_start DATE,
# MAGIC     time_period_end DATE,
# MAGIC     patient_count INT,
# MAGIC     consent_obtained BOOLEAN,
# MAGIC     commercial_use_approved BOOLEAN,
# MAGIC     identifiability_level STRING,
# MAGIC     data_quality_score INT,
# MAGIC     data_owner_email STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Real-world evidence datasets from external healthcare sources';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS adverse_events (
# MAGIC     ae_id STRING NOT NULL,
# MAGIC     trial_id STRING,
# MAGIC     patient_id STRING,
# MAGIC     event_term STRING,
# MAGIC     meddra_code STRING,
# MAGIC     severity_grade STRING,
# MAGIC     causality_assessment STRING,
# MAGIC     event_start_date DATE,
# MAGIC     event_end_date DATE,
# MAGIC     outcome STRING,
# MAGIC     serious_ae_flag BOOLEAN,
# MAGIC     regulatory_reportable BOOLEAN,
# MAGIC     reported_to_authorities ARRAY<STRING>,
# MAGIC     identifiability_level STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Adverse event reports from clinical trials and post-market surveillance';
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- PART 3: TABLE CREATION - REGULATORY COMPLIANCE SCHEMA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC USE SCHEMA hls_regulatory_compliance;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS regulatory_submissions (
# MAGIC     submission_id STRING NOT NULL,
# MAGIC     submission_type STRING,
# MAGIC     regulatory_authority STRING,
# MAGIC     molecule_name STRING,
# MAGIC     therapeutic_area STRING,
# MAGIC     submission_date DATE,
# MAGIC     approval_date DATE,
# MAGIC     submission_status STRING,
# MAGIC     dossier_location STRING,
# MAGIC     related_trials ARRAY<STRING>,
# MAGIC     primary_contact_email STRING,
# MAGIC     regulatory_lead_email STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Regulatory submission tracking for NDA, BLA, and other filings';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gxp_validated_systems (
# MAGIC     system_id STRING NOT NULL,
# MAGIC     system_name STRING,
# MAGIC     system_type STRING,
# MAGIC     gxp_classification STRING,
# MAGIC     validation_status STRING,
# MAGIC     validation_date DATE,
# MAGIC     revalidation_due_date DATE,
# MAGIC     vendor_name STRING,
# MAGIC     business_process STRING,
# MAGIC     regulatory_scope ARRAY<STRING>,
# MAGIC     cfr_part11_compliant BOOLEAN,
# MAGIC     eu_annex11_compliant BOOLEAN,
# MAGIC     system_owner_email STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'GxP validated system inventory and compliance tracking';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS audit_trail_logs (
# MAGIC     log_id STRING NOT NULL,
# MAGIC     system_id STRING,
# MAGIC     table_name STRING,
# MAGIC     action_type STRING,
# MAGIC     user_id STRING,
# MAGIC     timestamp TIMESTAMP,
# MAGIC     record_id STRING,
# MAGIC     field_changed STRING,
# MAGIC     old_value STRING,
# MAGIC     new_value STRING,
# MAGIC     reason_for_change STRING,
# MAGIC     gxp_critical BOOLEAN,
# MAGIC     created_date TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Audit trail logs for GxP compliance and regulatory inspection';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS retention_schedules (
# MAGIC     schedule_id STRING NOT NULL,
# MAGIC     data_category STRING,
# MAGIC     regulatory_basis STRING,
# MAGIC     retention_period_years INT,
# MAGIC     retention_trigger STRING,
# MAGIC     disposition_method STRING,
# MAGIC     geographic_scope STRING,
# MAGIC     legal_hold_applicable BOOLEAN,
# MAGIC     policy_reference STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Data retention schedules based on regulatory requirements';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS pharmacovigilance_reports (
# MAGIC     report_id STRING NOT NULL,
# MAGIC     report_type STRING,
# MAGIC     case_number STRING,
# MAGIC     molecule_name STRING,
# MAGIC     event_description STRING,
# MAGIC     meddra_preferred_term STRING,
# MAGIC     seriousness_criteria STRING,
# MAGIC     patient_age INT,
# MAGIC     patient_gender STRING,
# MAGIC     geographic_origin STRING,
# MAGIC     report_date DATE,
# MAGIC     regulatory_deadline DATE,
# MAGIC     submitted_to_authorities ARRAY<STRING>,
# MAGIC     report_status STRING,
# MAGIC     identifiability_level STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Pharmacovigilance ICSR and aggregate safety reports';
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- PART 4: TABLE CREATION - PATIENT PRIVACY SCHEMA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC USE SCHEMA hls_patient_privacy;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS consent_records (
# MAGIC     consent_id STRING NOT NULL,
# MAGIC     patient_id STRING,
# MAGIC     trial_id STRING,
# MAGIC     consent_date DATE,
# MAGIC     consent_version STRING,
# MAGIC     consent_scope STRING,
# MAGIC     primary_use_approved BOOLEAN,
# MAGIC     secondary_research_approved BOOLEAN,
# MAGIC     genetic_data_approved BOOLEAN,
# MAGIC     commercial_use_approved BOOLEAN,
# MAGIC     data_sharing_approved BOOLEAN,
# MAGIC     withdrawal_date DATE,
# MAGIC     consent_status STRING,
# MAGIC     geographic_jurisdiction STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Patient consent records for data usage and research participation';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS data_subject_requests (
# MAGIC     request_id STRING NOT NULL,
# MAGIC     request_type STRING,
# MAGIC     patient_id STRING,
# MAGIC     request_date DATE,
# MAGIC     completion_date DATE,
# MAGIC     request_status STRING,
# MAGIC     affected_systems ARRAY<STRING>,
# MAGIC     affected_datasets ARRAY<STRING>,
# MAGIC     regulatory_basis STRING,
# MAGIC     response_deadline DATE,
# MAGIC     assigned_to_email STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'GDPR and privacy rights requests (erasure, portability, access)';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS anonymization_metadata (
# MAGIC     anonymization_id STRING NOT NULL,
# MAGIC     source_dataset STRING,
# MAGIC     anonymized_dataset STRING,
# MAGIC     anonymization_method STRING,
# MAGIC     anonymization_date DATE,
# MAGIC     techniques_applied ARRAY<STRING>,
# MAGIC     re_identification_risk STRING,
# MAGIC     k_anonymity_value INT,
# MAGIC     expert_determination_performed BOOLEAN,
# MAGIC     hipaa_safe_harbor_compliant BOOLEAN,
# MAGIC     performed_by_email STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'De-identification and anonymization process tracking';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS special_category_data (
# MAGIC     category_id STRING NOT NULL,
# MAGIC     dataset_name STRING,
# MAGIC     data_category STRING,
# MAGIC     special_category_types ARRAY<STRING>,
# MAGIC     legal_basis STRING,
# MAGIC     processing_purpose STRING,
# MAGIC     data_minimization_applied BOOLEAN,
# MAGIC     encryption_required BOOLEAN,
# MAGIC     access_restrictions STRING,
# MAGIC     data_owner_email STRING,
# MAGIC     privacy_officer_email STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Special category data classification (genetic, health, pediatric)';
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- PART 5: TABLE CREATION - MANUFACTURING OPERATIONS SCHEMA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC USE SCHEMA hls_manufacturing_ops;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS batch_genealogy (
# MAGIC     batch_id STRING NOT NULL,
# MAGIC     product_sku STRING,
# MAGIC     manufacturing_site STRING,
# MAGIC     production_line STRING,
# MAGIC     production_date DATE,
# MAGIC     batch_size_units DOUBLE,
# MAGIC     batch_status STRING,
# MAGIC     parent_batches ARRAY<STRING>,
# MAGIC     child_batches ARRAY<STRING>,
# MAGIC     raw_material_lots ARRAY<STRING>,
# MAGIC     process_stage STRING,
# MAGIC     gmp_classification STRING,
# MAGIC     release_date DATE,
# MAGIC     expiry_date DATE,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Batch genealogy and lot tracking for manufacturing';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS quality_control_results (
# MAGIC     qc_id STRING NOT NULL,
# MAGIC     batch_id STRING,
# MAGIC     test_name STRING,
# MAGIC     test_type STRING,
# MAGIC     test_method STRING,
# MAGIC     specification_min DOUBLE,
# MAGIC     specification_max DOUBLE,
# MAGIC     result_value DOUBLE,
# MAGIC     result_unit STRING,
# MAGIC     pass_fail_status STRING,
# MAGIC     test_date DATE,
# MAGIC     tested_by STRING,
# MAGIC     approved_by STRING,
# MAGIC     out_of_specification BOOLEAN,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Quality control testing results and specifications';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS manufacturing_sites (
# MAGIC     site_id STRING NOT NULL,
# MAGIC     site_name STRING,
# MAGIC     site_location STRING,
# MAGIC     site_type STRING,
# MAGIC     gmp_certification BOOLEAN,
# MAGIC     certification_date DATE,
# MAGIC     regulatory_inspections ARRAY<STRING>,
# MAGIC     production_capabilities ARRAY<STRING>,
# MAGIC     equipment_inventory ARRAY<STRING>,
# MAGIC     capacity_units_per_year DOUBLE,
# MAGIC     site_manager_email STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Manufacturing site and facility master data';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS supply_chain_inventory (
# MAGIC     inventory_id STRING NOT NULL,
# MAGIC     item_type STRING,
# MAGIC     item_code STRING,
# MAGIC     item_description STRING,
# MAGIC     location STRING,
# MAGIC     quantity_on_hand DOUBLE,
# MAGIC     unit_of_measure STRING,
# MAGIC     lot_number STRING,
# MAGIC     expiry_date DATE,
# MAGIC     temperature_range STRING,
# MAGIC     cold_chain_required BOOLEAN,
# MAGIC     supplier_name STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Supply chain inventory for raw materials and finished goods';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS process_deviations (
# MAGIC     deviation_id STRING NOT NULL,
# MAGIC     batch_id STRING,
# MAGIC     deviation_date DATE,
# MAGIC     deviation_category STRING,
# MAGIC     severity_level STRING,
# MAGIC     description STRING,
# MAGIC     root_cause STRING,
# MAGIC     corrective_action STRING,
# MAGIC     preventive_action STRING,
# MAGIC     capa_id STRING,
# MAGIC     regulatory_reportable BOOLEAN,
# MAGIC     closure_date DATE,
# MAGIC     deviation_status STRING,
# MAGIC     owner_email STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Manufacturing process deviations and CAPA tracking';
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- PART 6: TABLE CREATION - COMMERCIAL ANALYTICS SCHEMA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC USE SCHEMA hls_commercial_analytics;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS prescription_trends (
# MAGIC     trend_id STRING NOT NULL,
# MAGIC     product_name STRING,
# MAGIC     therapeutic_area STRING,
# MAGIC     geographic_market STRING,
# MAGIC     time_period STRING,
# MAGIC     prescription_volume BIGINT,
# MAGIC     market_share_percent DOUBLE,
# MAGIC     growth_rate_percent DOUBLE,
# MAGIC     data_source STRING,
# MAGIC     patient_segment STRING,
# MAGIC     prescriber_specialty STRING,
# MAGIC     data_quality_score INT,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Prescription trend analytics by product and geography';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS market_access_contracts (
# MAGIC     contract_id STRING NOT NULL,
# MAGIC     payer_name STRING,
# MAGIC     payer_type STRING,
# MAGIC     product_name STRING,
# MAGIC     geographic_region STRING,
# MAGIC     contract_type STRING,
# MAGIC     contract_start_date DATE,
# MAGIC     contract_end_date DATE,
# MAGIC     pricing_tier STRING,
# MAGIC     rebate_percent DOUBLE,
# MAGIC     volume_commitment BIGINT,
# MAGIC     outcomes_based_component BOOLEAN,
# MAGIC     commercial_sensitivity STRING,
# MAGIC     contract_owner_email STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Payer contracts and market access agreements';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS hcp_engagement (
# MAGIC     engagement_id STRING NOT NULL,
# MAGIC     hcp_id STRING,
# MAGIC     hcp_specialty STRING,
# MAGIC     engagement_type STRING,
# MAGIC     engagement_date DATE,
# MAGIC     product_discussed STRING,
# MAGIC     therapeutic_area STRING,
# MAGIC     channel STRING,
# MAGIC     interaction_duration_min INT,
# MAGIC     sentiment_score INT,
# MAGIC     follow_up_required BOOLEAN,
# MAGIC     sales_rep_id STRING,
# MAGIC     privacy_consent_obtained BOOLEAN,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Healthcare provider engagement and interaction tracking';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS competitive_intelligence (
# MAGIC     intel_id STRING NOT NULL,
# MAGIC     competitor_name STRING,
# MAGIC     competitor_product STRING,
# MAGIC     therapeutic_area STRING,
# MAGIC     intelligence_type STRING,
# MAGIC     intelligence_summary STRING,
# MAGIC     source_type STRING,
# MAGIC     geographic_scope STRING,
# MAGIC     confidence_level STRING,
# MAGIC     business_impact STRING,
# MAGIC     action_required BOOLEAN,
# MAGIC     assigned_to_email STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Competitive intelligence and market landscape analysis';
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- PART 7: TABLE CREATION - INFRASTRUCTURE SCHEMA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC USE SCHEMA hls_infrastructure;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS source_systems_catalog (
# MAGIC     system_id STRING NOT NULL,
# MAGIC     system_name STRING,
# MAGIC     system_type STRING,
# MAGIC     vendor_name STRING,
# MAGIC     business_domain STRING,
# MAGIC     system_owner_email STRING,
# MAGIC     technical_contact_email STRING,
# MAGIC     integration_method STRING,
# MAGIC     data_refresh_frequency STRING,
# MAGIC     sla_uptime_percent DOUBLE,
# MAGIC     criticality_tier STRING,
# MAGIC     gxp_validated BOOLEAN,
# MAGIC     environment STRING,
# MAGIC     connection_string STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Source system catalog and integration metadata';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS data_pipelines (
# MAGIC     pipeline_id STRING NOT NULL,
# MAGIC     pipeline_name STRING,
# MAGIC     pipeline_type STRING,
# MAGIC     source_system STRING,
# MAGIC     target_schema STRING,
# MAGIC     target_table STRING,
# MAGIC     orchestration_tool STRING,
# MAGIC     schedule_expression STRING,
# MAGIC     data_layer STRING,
# MAGIC     transformation_logic STRING,
# MAGIC     row_count BIGINT,
# MAGIC     last_run_timestamp TIMESTAMP,
# MAGIC     last_run_status STRING,
# MAGIC     pipeline_owner_email STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Data pipeline metadata and orchestration tracking';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS data_quality_metrics (
# MAGIC     metric_id STRING NOT NULL,
# MAGIC     table_name STRING,
# MAGIC     metric_type STRING,
# MAGIC     metric_name STRING,
# MAGIC     metric_value DOUBLE,
# MAGIC     threshold_min DOUBLE,
# MAGIC     threshold_max DOUBLE,
# MAGIC     quality_check_status STRING,
# MAGIC     measurement_timestamp TIMESTAMP,
# MAGIC     dimension STRING,
# MAGIC     issue_count INT,
# MAGIC     issue_details STRING,
# MAGIC     created_date TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Data quality monitoring results and metrics';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS integration_interfaces (
# MAGIC     interface_id STRING NOT NULL,
# MAGIC     interface_name STRING,
# MAGIC     interface_type STRING,
# MAGIC     source_system STRING,
# MAGIC     target_system STRING,
# MAGIC     protocol STRING,
# MAGIC     endpoint_url STRING,
# MAGIC     authentication_method STRING,
# MAGIC     data_format STRING,
# MAGIC     message_volume_daily BIGINT,
# MAGIC     sla_response_time_ms INT,
# MAGIC     interface_owner_email STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'API and integration interface specifications';
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- PART 8: TABLE CREATION - GOVERNANCE SCHEMA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC USE SCHEMA hls_governance;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS data_ownership_registry (
# MAGIC     ownership_id STRING NOT NULL,
# MAGIC     data_asset_name STRING,
# MAGIC     asset_type STRING,
# MAGIC     data_domain STRING,
# MAGIC     data_owner_email STRING,
# MAGIC     technical_steward_email STRING,
# MAGIC     business_steward_email STRING,
# MAGIC     sme_contact_email STRING,
# MAGIC     governance_tier STRING,
# MAGIC     stewardship_maturity STRING,
# MAGIC     ownership_start_date DATE,
# MAGIC     review_frequency STRING,
# MAGIC     last_review_date DATE,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Data ownership and stewardship registry';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS data_classification (
# MAGIC     classification_id STRING NOT NULL,
# MAGIC     data_asset_name STRING,
# MAGIC     classification_level STRING,
# MAGIC     criticality_rating STRING,
# MAGIC     sensitivity_category STRING,
# MAGIC     regulatory_scope ARRAY<STRING>,
# MAGIC     encryption_required BOOLEAN,
# MAGIC     access_control_level STRING,
# MAGIC     retention_category STRING,
# MAGIC     classification_date DATE,
# MAGIC     classified_by_email STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Data asset classification and sensitivity ratings';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS governance_policies (
# MAGIC     policy_id STRING NOT NULL,
# MAGIC     policy_name STRING,
# MAGIC     policy_category STRING,
# MAGIC     policy_description STRING,
# MAGIC     policy_owner_email STRING,
# MAGIC     approval_date DATE,
# MAGIC     effective_date DATE,
# MAGIC     review_date DATE,
# MAGIC     policy_status STRING,
# MAGIC     applicable_domains ARRAY<STRING>,
# MAGIC     compliance_mandatory BOOLEAN,
# MAGIC     policy_document_url STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Data governance policies and standards';
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- PART 9: TABLE CREATION - AI/ML ANALYTICS SCHEMA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC USE SCHEMA hls_ai_ml_analytics;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ml_models_registry (
# MAGIC     model_id STRING NOT NULL,
# MAGIC     model_name STRING,
# MAGIC     use_case_type STRING,
# MAGIC     model_type STRING,
# MAGIC     algorithm STRING,
# MAGIC     model_stage STRING,
# MAGIC     model_version STRING,
# MAGIC     training_dataset_id STRING,
# MAGIC     performance_metric STRING,
# MAGIC     performance_value DOUBLE,
# MAGIC     model_risk_class STRING,
# MAGIC     bias_assessment_status STRING,
# MAGIC     deployed_date DATE,
# MAGIC     mlflow_run_id STRING,
# MAGIC     model_owner_email STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'ML model registry with metadata and performance tracking';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS feature_stores (
# MAGIC     feature_id STRING NOT NULL,
# MAGIC     feature_name STRING,
# MAGIC     feature_group STRING,
# MAGIC     feature_type STRING,
# MAGIC     data_source STRING,
# MAGIC     calculation_logic STRING,
# MAGIC     feature_importance_score DOUBLE,
# MAGIC     models_using_feature ARRAY<STRING>,
# MAGIC     feature_quality_score INT,
# MAGIC     refresh_frequency STRING,
# MAGIC     created_by_email STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Feature engineering catalog for ML pipelines';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS ml_training_datasets (
# MAGIC     training_dataset_id STRING NOT NULL,
# MAGIC     dataset_name STRING,
# MAGIC     source_tables ARRAY<STRING>,
# MAGIC     use_case_type STRING,
# MAGIC     therapeutic_area STRING,
# MAGIC     row_count BIGINT,
# MAGIC     feature_count INT,
# MAGIC     target_variable STRING,
# MAGIC     data_split_strategy STRING,
# MAGIC     consent_approved BOOLEAN,
# MAGIC     identifiability_level STRING,
# MAGIC     bias_mitigation_applied BOOLEAN,
# MAGIC     dataset_version STRING,
# MAGIC     created_by_email STRING,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'ML training dataset lineage and metadata';
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- PART 10: TABLE CREATION - REFERENCE DATA SCHEMA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC USE SCHEMA hls_reference_data;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS therapeutic_areas (
# MAGIC     ta_id STRING NOT NULL,
# MAGIC     ta_name STRING,
# MAGIC     ta_code STRING,
# MAGIC     ta_description STRING,
# MAGIC     parent_ta_id STRING,
# MAGIC     therapeutic_portfolio STRING,
# MAGIC     active BOOLEAN,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Therapeutic area master data';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS controlled_vocabularies (
# MAGIC     vocab_id STRING NOT NULL,
# MAGIC     vocabulary_name STRING,
# MAGIC     vocabulary_version STRING,
# MAGIC     term_code STRING,
# MAGIC     term_name STRING,
# MAGIC     term_definition STRING,
# MAGIC     parent_code STRING,
# MAGIC     term_category STRING,
# MAGIC     effective_date DATE,
# MAGIC     deprecated BOOLEAN,
# MAGIC     created_date TIMESTAMP,
# MAGIC     last_updated TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC COMMENT 'Controlled vocabularies and medical coding standards (MedDRA, ICD, SNOMED)';

# COMMAND ----------

# DBTITLE 1,insert data
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- DATA GENERATION SCRIPTS - GSK ENTERPRISE CATALOG
# MAGIC -- ============================================================================
# MAGIC
# MAGIC USE CATALOG sean_zhang_catalog;
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- REFERENCE DATA - POPULATE FIRST (Dependencies)
# MAGIC -- ============================================================================
# MAGIC
# MAGIC USE SCHEMA hls_reference_data;
# MAGIC
# MAGIC -- Insert Therapeutic Areas
# MAGIC INSERT INTO reference_data.therapeutic_areas (ta_id, ta_name, ta_code, ta_description, parent_ta_id, therapeutic_portfolio, active, created_date, last_updated) VALUES
# MAGIC ('TA001', 'Oncology', 'ONC', 'Cancer therapeutics and treatments', NULL, 'Specialty Care', true, current_timestamp(), current_timestamp()),
# MAGIC ('TA002', 'Vaccines', 'VAC', 'Preventive vaccines and immunizations', NULL, 'Vaccines & Specialty', true, current_timestamp(), current_timestamp()),
# MAGIC ('TA003', 'HIV', 'HIV', 'HIV/AIDS treatment and prevention', NULL, 'Specialty Care', true, current_timestamp(), current_timestamp()),
# MAGIC ('TA004', 'Respiratory', 'RESP', 'Respiratory disease treatments including asthma and COPD', NULL, 'General Medicines', true, current_timestamp(), current_timestamp()),
# MAGIC ('TA005', 'Immunology', 'IMM', 'Immune system disorders and autoimmune diseases', NULL, 'Specialty Care', true, current_timestamp(), current_timestamp()),
# MAGIC ('TA006', 'Rare Diseases', 'RARE', 'Orphan drugs and rare disease treatments', NULL, 'Specialty Care', true, current_timestamp(), current_timestamp()),
# MAGIC ('TA007', 'Infectious Diseases', 'INF', 'Antibiotics and anti-infective therapies', NULL, 'General Medicines', true, current_timestamp(), current_timestamp()),
# MAGIC ('TA008', 'Dermatology', 'DERM', 'Skin conditions and dermatological treatments', NULL, 'General Medicines', true, current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert Controlled Vocabularies (MedDRA, ICD codes)
# MAGIC INSERT INTO controlled_vocabularies VALUES
# MAGIC ('CV001', 'MedDRA', 'v25.0', 'PT10000132', 'Acute myocardial infarction', 'Heart attack - primary term', 'SOC10007541', 'Cardiac disorders', '2023-03-01', false, current_timestamp(), current_timestamp()),
# MAGIC ('CV002', 'MedDRA', 'v25.0', 'PT10002855', 'Anaphylactic reaction', 'Severe allergic reaction', 'SOC10018065', 'Immune system disorders', '2023-03-01', false, current_timestamp(), current_timestamp()),
# MAGIC ('CV003', 'MedDRA', 'v25.0', 'PT10028813', 'Nausea', 'Feeling of sickness', 'SOC10017947', 'Gastrointestinal disorders', '2023-03-01', false, current_timestamp(), current_timestamp()),
# MAGIC ('CV004', 'MedDRA', 'v25.0', 'PT10019211', 'Headache', 'Pain in head region', 'SOC10029205', 'Nervous system disorders', '2023-03-01', false, current_timestamp(), current_timestamp()),
# MAGIC ('CV005', 'ICD-10', '2023', 'C34.90', 'Malignant neoplasm of bronchus and lung, unspecified', 'Lung cancer unspecified', 'C00-D49', 'Neoplasms', '2023-01-01', false, current_timestamp(), current_timestamp()),
# MAGIC ('CV006', 'ICD-10', '2023', 'J45.909', 'Unspecified asthma, uncomplicated', 'Asthma diagnosis', 'J00-J99', 'Respiratory system diseases', '2023-01-01', false, current_timestamp(), current_timestamp()),
# MAGIC ('CV007', 'SNOMED CT', '2023-07', '84114007', 'Heart failure', 'Cardiac insufficiency', '404684003', 'Clinical finding', '2023-07-01', false, current_timestamp(), current_timestamp()),
# MAGIC ('CV008', 'LOINC', 'v2.74', '2093-3', 'Cholesterol [Mass/volume] in Serum or Plasma', 'Total cholesterol lab test', '2085-9', 'Laboratory observations', '2023-06-01', false, current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- CLINICAL RESEARCH DATA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC USE SCHEMA hls_clinical_research;
# MAGIC
# MAGIC -- Insert Clinical Trials
# MAGIC INSERT INTO clinical_trials VALUES
# MAGIC ('CT001', 'Phase III Study of Novel PD-L1 Inhibitor in NSCLC', 'GSK-ONC-2301', 'Oncology', 'Phase_III', 'GSK3801', 'Progression-free survival at 12 months', 450, 428, '2023-01-15', '2025-12-31', 'Dr. Sarah Chen', 'GSK', 'Ongoing', 'NCT05234567', 'clinical.data.owner@gsk.com', 'oncology.steward@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('CT002', 'RSV Vaccine Efficacy Trial in Older Adults', 'GSK-VAC-2205', 'Vaccines', 'Phase_III', 'GSK4502', 'Prevention of RSV-related hospitalization', 25000, 24856, '2022-05-10', '2024-11-30', 'Dr. Michael Roberts', 'GSK', 'Completed', 'NCT04886596', 'clinical.data.owner@gsk.com', 'vaccines.steward@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('CT003', 'Long-Acting HIV Treatment Combination Study', 'GSK-HIV-2304', 'HIV', 'Phase_III', 'GSK5901', 'Viral suppression at 48 weeks', 780, 756, '2023-04-01', '2026-03-31', 'Dr. James Wilson', 'GSK', 'Ongoing', 'NCT05445678', 'clinical.data.owner@gsk.com', 'hiv.steward@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('CT004', 'Severe Asthma Biologic Therapy Study', 'GSK-RESP-2209', 'Respiratory', 'Phase_III', 'GSK6201', 'Reduction in exacerbation rate', 890, 867, '2022-09-15', '2025-06-30', 'Dr. Emily Thompson', 'GSK', 'Ongoing', 'NCT05112233', 'clinical.data.owner@gsk.com', 'respiratory.steward@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('CT005', 'Pediatric Meningitis Vaccine Trial', 'GSK-VAC-2308', 'Vaccines', 'Phase_II', 'GSK4701', 'Immunogenicity and safety endpoints', 1200, 1187, '2023-08-01', '2025-07-31', 'Dr. Robert Martinez', 'GSK', 'Ongoing', 'NCT05667788', 'clinical.data.owner@gsk.com', 'vaccines.steward@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('CT006', 'Rare Disease Gene Therapy Study', 'GSK-RARE-2307', 'Rare Diseases', 'Phase_I', 'GSK7801', 'Safety and tolerability', 45, 42, '2023-07-20', '2025-12-31', 'Dr. Anna Kowalski', 'GSK', 'Ongoing', 'NCT05778899', 'clinical.data.owner@gsk.com', 'rare.diseases.steward@gsk.com', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert Patient Cohorts
# MAGIC INSERT INTO patient_cohorts VALUES
# MAGIC ('COH001', 'CT001', 'Stage III/IV NSCLC Previously Treated', 428, 'Confirmed NSCLC, prior platinum therapy, ECOG 0-1', 'Active brain metastases, prior immunotherapy', 'US,EU,APAC', 45, 85, '{"Male": "58%", "Female": "42%"}', '{"Caucasian": "62%", "Asian": "28%", "Other": "10%"}', 'Primary_Only', 'Pseudonymized', false, 'Restricted', current_timestamp(), current_timestamp()),
# MAGIC ('COH002', 'CT002', 'Adults 60+ Years Without Prior RSV Vaccination', 24856, 'Age ≥60 years, no prior RSV vaccine', 'Immunocompromised, active respiratory infection', 'US,EU,UK,Canada', 60, 95, '{"Male": "47%", "Female": "53%"}', '{"Caucasian": "71%", "African American": "15%", "Hispanic": "10%", "Other": "4%"}', 'Broad_Consent', 'De_Identified', false, 'Confidential', current_timestamp(), current_timestamp()),
# MAGIC ('COH003', 'CT003', 'Treatment-Experienced HIV Patients', 756, 'Documented HIV-1, viral load <50 copies/mL on current ART', 'Hepatitis B co-infection, resistance mutations', 'Global', 22, 68, '{"Male": "76%", "Female": "23%", "Other": "1%"}', '{"Diverse": "100%"}', 'Secondary_Research', 'Pseudonymized', false, 'Restricted', current_timestamp(), current_timestamp()),
# MAGIC ('COH004', 'CT004', 'Severe Uncontrolled Asthma Patients', 867, 'Severe asthma, ≥2 exacerbations in past year', 'Other chronic respiratory diseases, smokers', 'US,EU,Japan', 18, 75, '{"Male": "43%", "Female": "57%"}', '{"Caucasian": "68%", "Asian": "22%", "Other": "10%"}', 'Primary_Only', 'Pseudonymized', false, 'Confidential', current_timestamp(), current_timestamp()),
# MAGIC ('COH005', 'CT005', 'Pediatric Population 2-10 Years', 1187, 'Healthy children aged 2-10 years', 'Immunodeficiency, prior meningitis', 'Africa,Asia,Latin America', 2, 10, '{"Male": "51%", "Female": "49%"}', '{"African": "45%", "Asian": "35%", "Hispanic": "20%"}', 'Broad_Consent', 'De_Identified', true, 'Restricted', current_timestamp(), current_timestamp()),
# MAGIC ('COH006', 'CT006', 'Rare Genetic Disorder Patients', 42, 'Confirmed genetic mutation, no prior gene therapy', 'Severe organ dysfunction, pregnancy', 'US,EU', 5, 45, '{"Male": "52%", "Female": "48%"}', '{"Caucasian": "71%", "Other": "29%"}', 'Secondary_Research', 'Pseudonymized', true, 'Restricted', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert Clinical Endpoints
# MAGIC INSERT INTO clinical_endpoints VALUES
# MAGIC ('EP001', 'CT001', 'Progression-Free Survival', 'Primary', 'Months', 'RECIST v1.1 imaging assessment', 'Every 6 weeks', 12.0, 'Kaplan-Meier, Log-rank test', 'Active', 'Imaging', current_timestamp(), current_timestamp()),
# MAGIC ('EP002', 'CT001', 'Overall Survival', 'Secondary', 'Months', 'Clinical follow-up', 'Ongoing until death/study end', 24.0, 'Kaplan-Meier, Cox regression', 'Active', 'Clinical_Lab', current_timestamp(), current_timestamp()),
# MAGIC ('EP003', 'CT002', 'RSV-Related Hospitalization Rate', 'Primary', 'Percentage', 'Confirmed RSV infection requiring hospitalization', 'Throughout RSV season', 0.8, 'Risk difference, 95% CI', 'Completed', 'Clinical_Lab', current_timestamp(), current_timestamp()),
# MAGIC ('EP004', 'CT003', 'Viral Suppression at 48 Weeks', 'Primary', 'Percentage', 'HIV-1 RNA <50 copies/mL', 'Week 48', 95.0, 'Proportion difference', 'Active', 'Clinical_Lab', current_timestamp(), current_timestamp()),
# MAGIC ('EP005', 'CT004', 'Annual Exacerbation Rate', 'Primary', 'Count per patient-year', 'Clinically significant asthma exacerbations', 'Continuous monitoring', 0.5, 'Negative binomial regression', 'Active', 'Clinical_Lab', current_timestamp(), current_timestamp()),
# MAGIC ('EP006', 'CT005', 'Seroprotection Rate', 'Primary', 'Percentage', 'Antibody titers above protective threshold', 'Day 28 post-vaccination', 90.0, 'Proportion with 95% CI', 'Active', 'Clinical_Lab', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert Genomics Datasets
# MAGIC INSERT INTO genomics_datasets VALUES
# MAGIC ('GEN001', 'NSCLC Tumor WGS Dataset', 'CT001', 'Illumina NovaSeq 6000', 'Genomics', 'GRCh38', 385, '100x', 4567890, 'VCF', 's3://gsk-genomics/nsclc-wgs/', 8750.5, 'Secondary_Research', 'Anonymized', true, true, 'genomics.data@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('GEN002', 'HIV Resistance Mutation Analysis', 'CT003', 'Illumina MiSeq', 'Genomics', 'HIV-1 Reference', 756, '1000x', 125000, 'FASTQ', 's3://gsk-genomics/hiv-resistance/', 145.2, 'Primary_Only', 'Pseudonymized', true, false, 'genomics.data@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('GEN003', 'Rare Disease Whole Exome Sequencing', 'CT006', 'Illumina NovaSeq 6000', 'Genomics', 'GRCh38', 42, '150x', 892000, 'VCF', 's3://gsk-genomics/rare-disease-wes/', 287.8, 'Secondary_Research', 'Anonymized', true, true, 'genomics.data@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('GEN004', 'Asthma Pharmacogenomics Panel', 'CT004', 'ThermoFisher Axiom', 'Genomics', 'GRCh38', 867, 'Genotyping Array', 1250000, 'PLINK', 's3://gsk-genomics/asthma-pgx/', 34.5, 'Secondary_Research', 'Pseudonymized', true, true, 'genomics.data@gsk.com', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert Real-World Evidence
# MAGIC INSERT INTO real_world_evidence VALUES
# MAGIC ('RWE001', 'US Oncology Claims Database', 'Optum Clinformatics', 'Oncology', 'Adult cancer patients', 'Claims_Data', 'US', '2018-01-01', '2023-12-31', 1250000, true, true, 'De_Identified', 88, 'rwe.data@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('RWE002', 'UK Primary Care Respiratory Cohort', 'CPRD GOLD', 'Respiratory', 'Asthma and COPD patients', 'EHR_Data', 'UK', '2015-01-01', '2023-12-31', 450000, true, true, 'Pseudonymized', 92, 'rwe.data@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('RWE003', 'European HIV Treatment Registry', 'EuroSIDA', 'HIV', 'HIV-positive adults on ART', 'Registry_Data', 'EU', '2010-01-01', '2023-12-31', 25000, true, false, 'Pseudonymized', 95, 'rwe.data@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('RWE004', 'Global Vaccine Safety Surveillance', 'Brighton Collaboration', 'Vaccines', 'Vaccinated populations', 'Safety_Surveillance', 'Global', '2020-01-01', '2024-01-15', 15000000, true, false, 'Anonymized', 85, 'rwe.data@gsk.com', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert Adverse Events
# MAGIC INSERT INTO adverse_events VALUES
# MAGIC ('AE001', 'CT001', 'PT001234', 'Immune-mediated pneumonitis', 'PT10035598', 'Grade 3', 'Possibly Related', '2024-03-15', '2024-04-20', 'Recovered', true, true, array('FDA', 'EMA'), 'Pseudonymized', current_timestamp(), current_timestamp()),
# MAGIC ('AE002', 'CT001', 'PT001567', 'Diarrhea', 'PT10012735', 'Grade 2', 'Probably Related', '2024-02-10', '2024-02-18', 'Recovered', false, false, array(), 'Pseudonymized', current_timestamp(), current_timestamp()),
# MAGIC ('AE003', 'CT002', 'PT005678', 'Injection site pain', 'PT10022086', 'Grade 1', 'Definitely Related', '2023-06-22', '2023-06-24', 'Recovered', false, false, array(), 'De_Identified', current_timestamp(), current_timestamp()),
# MAGIC ('AE004', 'CT003', 'PT007890', 'Elevated liver enzymes', 'PT10001675', 'Grade 2', 'Possibly Related', '2024-01-08', '2024-02-15', 'Recovered', false, true, array('FDA'), 'Pseudonymized', current_timestamp(), current_timestamp()),
# MAGIC ('AE005', 'CT004', 'PT009123', 'Anaphylaxis', 'PT10002198', 'Grade 4', 'Unrelated', '2023-11-30', '2023-12-01', 'Recovered', true, true, array('FDA', 'EMA', 'MHRA'), 'Pseudonymized', current_timestamp(), current_timestamp()),
# MAGIC ('AE006', 'CT005', 'PT012345', 'Fever', 'PT10016558', 'Grade 1', 'Definitely Related', '2024-01-12', '2024-01-13', 'Recovered', false, false, array(), 'De_Identified', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- REGULATORY COMPLIANCE DATA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC USE SCHEMA hls_regulatory_compliance;
# MAGIC
# MAGIC -- Insert Regulatory Submissions
# MAGIC INSERT INTO regulatory_submissions VALUES
# MAGIC ('SUB001', 'NDA', 'FDA', 'GSK3801', 'Oncology', '2024-06-15', NULL, 'Under_Review', 's3://gsk-regulatory/nda-gsk3801/', array('CT001'), 'regulatory.affairs@gsk.com', 'regulatory.lead.oncology@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('SUB002', 'BLA', 'FDA', 'GSK4502', 'Vaccines', '2024-02-28', '2024-11-20', 'Approved', 's3://gsk-regulatory/bla-gsk4502/', array('CT002'), 'regulatory.affairs@gsk.com', 'regulatory.lead.vaccines@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('SUB003', 'MAA', 'EMA', 'GSK3801', 'Oncology', '2024-07-10', NULL, 'Under_Review', 's3://gsk-regulatory/maa-gsk3801/', array('CT001'), 'regulatory.affairs@gsk.com', 'regulatory.lead.oncology@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('SUB004', 'NDA', 'FDA', 'GSK5901', 'HIV', '2025-01-05', NULL, 'Pre_Submission', 's3://gsk-regulatory/nda-gsk5901/', array('CT003'), 'regulatory.affairs@gsk.com', 'regulatory.lead.hiv@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('SUB005', 'BLA', 'FDA', 'GSK4701', 'Vaccines', '2023-09-12', '2024-05-08', 'Approved', 's3://gsk-regulatory/bla-gsk4701/', array('CT005'), 'regulatory.affairs@gsk.com', 'regulatory.lead.vaccines@gsk.com', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert GxP Validated Systems
# MAGIC INSERT INTO gxp_validated_systems VALUES
# MAGIC ('SYS001', 'Medidata Rave EDC', 'Clinical Trial Data Capture', 'GCP', 'Validated', '2023-06-15', '2026-06-15', 'Medidata Solutions', 'Clinical Data Management', array('FDA', 'EMA', 'MHRA'), true, true, 'clinical.systems@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('SYS002', 'SAP MES Manufacturing Suite', 'Manufacturing Execution System', 'GMP', 'Validated', '2022-03-20', '2025-03-20', 'SAP', 'Manufacturing Operations', array('FDA', 'EMA'), true, true, 'manufacturing.systems@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('SYS003', 'LabWare LIMS', 'Laboratory Information Management', 'GLP', 'Validated', '2023-01-10', '2026-01-10', 'LabWare', 'Quality Control Testing', array('FDA', 'EMA'), true, true, 'quality.systems@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('SYS004', 'Argus Safety Database', 'Pharmacovigilance', 'GVP', 'Validated', '2023-09-01', '2026-09-01', 'Oracle', 'Drug Safety', array('FDA', 'EMA', 'PMDA'), true, true, 'safety.systems@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('SYS005', 'Veeva Vault QMS', 'Quality Management System', 'GMP', 'Under_Validation', '2024-11-15', '2027-11-15', 'Veeva Systems', 'Quality & Compliance', array('FDA', 'EMA'), true, true, 'quality.systems@gsk.com', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert Audit Trail Logs (sample)
# MAGIC INSERT INTO audit_trail_logs VALUES
# MAGIC ('LOG001', 'SYS001', 'clinical_trials', 'UPDATE', 'user123@gsk.com', '2024-01-15 14:23:45', 'CT001', 'enrollment_actual', '420', '428', 'Updated with latest enrollment count', true, current_timestamp()),
# MAGIC ('LOG002', 'SYS002', 'batch_genealogy', 'INSERT', 'user456@gsk.com', '2024-01-16 09:15:22', 'BATCH001', NULL, NULL, 'BATCH001 record created', 'New batch production record', true, current_timestamp()),
# MAGIC ('LOG003', 'SYS003', 'quality_control_results', 'UPDATE', 'user789@gsk.com', '2024-01-16 11:45:33', 'QC001', 'pass_fail_status', 'PENDING', 'PASS', 'QC test completed successfully', true, current_timestamp()),
# MAGIC ('LOG004', 'SYS004', 'adverse_events', 'INSERT', 'user234@gsk.com', '2024-01-17 08:30:12', 'AE001', NULL, NULL, 'New adverse event reported', 'SAE reported from site 105', true, current_timestamp()),
# MAGIC ('LOG005', 'SYS001', 'patient_cohorts', 'UPDATE', 'user567@gsk.com', '2024-01-17 16:20:55', 'COH001', 'patient_count', '425', '428', 'Patient enrollment update', true, current_timestamp());
# MAGIC
# MAGIC -- Insert Retention Schedules
# MAGIC INSERT INTO retention_schedules VALUES
# MAGIC ('RET001', 'Clinical Trial Data', 'ICH GCP E6(R2)', 25, 'Study completion or regulatory approval + 25 years', 'Secure_Destruction', 'Global', false, 'POL-RET-001', current_timestamp(), current_timestamp()),
# MAGIC ('RET002', 'Manufacturing Batch Records', '21 CFR Part 211', 1, 'Batch expiry + 1 year', 'Secure_Destruction', 'US', false, 'POL-RET-002', current_timestamp(), current_timestamp()),
# MAGIC ('RET003', 'Pharmacovigilance ICSRs', 'EU Directive 2001/83/EC', 15, 'Product discontinuation + 15 years', 'Secure_Destruction', 'EU', false, 'POL-RET-003', current_timestamp(), current_timestamp()),
# MAGIC ('RET004', 'GxP System Validation Records', 'FDA 21 CFR Part 11', 10, 'System retirement + 10 years', 'Secure_Destruction', 'US', false, 'POL-RET-004', current_timestamp(), current_timestamp()),
# MAGIC ('RET005', 'Patient Consent Forms', 'GDPR Article 17', 7, 'Trial completion + 7 years or patient request', 'Secure_Erasure', 'EU', false, 'POL-RET-005', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert Pharmacovigilance Reports
# MAGIC INSERT INTO pharmacovigilance_reports VALUES
# MAGIC ('PV001', 'ICSR', 'ICSR-2024-001234', 'GSK3801', 'Immune-mediated pneumonitis in NSCLC patient', 'PT10035598', 'Death', 67, 'Male', 'US', '2024-03-16', '2024-03-30', array('FDA'), 'Submitted', 'Pseudonymized', current_timestamp(), current_timestamp()),
# MAGIC ('PV002', 'ICSR', 'ICSR-2023-005678', 'GSK4502', 'Anaphylaxis post-vaccination', 'PT10002198', 'Life-threatening', 72, 'Female', 'UK', '2023-12-01', '2023-12-15', array('EMA', 'MHRA'), 'Submitted', 'Pseudonymized', current_timestamp(), current_timestamp()),
# MAGIC ('PV003', 'Aggregate', 'PSUR-2024-Q1-GSK5901', 'GSK5901', 'Quarterly safety update for HIV therapy', 'Multiple', 'N/A', NULL, NULL, 'Global', '2024-01-05', '2024-01-20', array('FDA', 'EMA'), 'Submitted', 'Anonymized', current_timestamp(), current_timestamp()),
# MAGIC ('PV004', 'ICSR', 'ICSR-2024-002345', 'GSK6201', 'Elevated liver enzymes', 'PT10001675', 'Hospitalization', 54, 'Male', 'Germany', '2024-01-09', '2024-01-23', array('EMA'), 'Submitted', 'Pseudonymized', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- PATIENT PRIVACY DATA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC USE SCHEMA hls_patient_privacy;
# MAGIC
# MAGIC -- Insert Consent Records
# MAGIC INSERT INTO consent_records VALUES
# MAGIC ('CON001', 'PT001234', 'CT001', '2023-02-10', 'v2.1', 'Secondary_Research', true, true, true, false, false, NULL, 'Active', 'US', current_timestamp(), current_timestamp()),
# MAGIC ('CON002', 'PT005678', 'CT002', '2022-06-15', 'v1.5', 'Broad_Consent', true, true, false, false, true, NULL, 'Active', 'UK', current_timestamp(), current_timestamp()),
# MAGIC ('CON003', 'PT007890', 'CT003', '2023-05-20', 'v3.0', 'Secondary_Research', true, true, true, false, true, NULL, 'Active', 'France', current_timestamp(), current_timestamp()),
# MAGIC ('CON004', 'PT012345', 'CT005', '2023-09-12', 'v2.3', 'Broad_Consent', true, true, true, true, true, NULL, 'Active', 'Kenya', current_timestamp(), current_timestamp()),
# MAGIC ('CON005', 'PT098765', 'CT001', '2023-03-22', 'v2.1', 'Primary_Only', true, false, false, false, false, '2024-01-10', 'Withdrawn', 'US', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert Data Subject Requests
# MAGIC INSERT INTO data_subject_requests VALUES
# MAGIC ('DSR001', 'Right_to_Erasure', 'PT098765', '2024-01-10', '2024-02-08', 'Completed', array('SYS001', 'SYS004'), array('clinical_trials', 'adverse_events'), 'GDPR Article 17', '2024-02-09', 'privacy.officer@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('DSR002', 'Right_to_Access', 'PT012456', '2024-01-15', NULL, 'In_Progress', array('SYS001'), array('patient_cohorts'), 'GDPR Article 15', '2024-02-14', 'privacy.officer@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('DSR003', 'Right_to_Portability', 'PT023456', '2023-12-20', '2024-01-18', 'Completed', array('SYS001', 'SYS003'), array('clinical_endpoints', 'genomics_datasets'), 'GDPR Article 20', '2024-01-19', 'privacy.officer@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('DSR004', 'Right_to_Rectification', 'PT034567', '2024-01-08', '2024-01-22', 'Completed', array('SYS001'), array('patient_cohorts'), 'GDPR Article 16', '2024-02-07', 'privacy.officer@gsk.com', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert Anonymization Metadata
# MAGIC INSERT INTO anonymization_metadata VALUES
# MAGIC ('ANON001', 'clinical_research.patient_cohorts', 'analytics.anonymized_cohorts', 'Expert_Determination', '2023-12-15', array('Identifier_Removal', 'Generalization', 'Perturbation'), 'Very_Low', 50, true, true, 'privacy.team@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('ANON002', 'clinical_research.genomics_datasets', 'analytics.genomics_anonymized', 'Safe_Harbor', '2024-01-10', array('Identifier_Removal', 'Date_Shifting', 'Aggregation'), 'Low', 20, true, true, 'privacy.team@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('ANON003', 'clinical_research.real_world_evidence', 'analytics.rwe_anonymized', 'K_Anonymity', '2023-11-20', array('Suppression', 'Generalization'), 'Low', 100, true, false, 'privacy.team@gsk.com', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert Special Category Data
# MAGIC INSERT INTO special_category_data VALUES
# MAGIC ('SPC001', 'genomics_datasets', 'Genetic_Data', array('Genetic'), 'GDPR Article 9(2)(j)', 'Scientific Research', true, true, 'Role_Based_Access_Only', 'genomics.data@gsk.com', 'privacy.officer@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('SPC002', 'patient_cohorts', 'Health_Data', array('Health', 'Pediatric'), 'GDPR Article 9(2)(j)', 'Clinical Trial', true, true, 'Multi_Factor_Auth_Required', 'clinical.data.owner@gsk.com', 'privacy.officer@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('SPC003', 'adverse_events', 'Health_Safety_Data', array('Health'), 'GDPR Article 9(2)(i)', 'Public Health & Safety', true, true, 'Restricted_Access', 'safety.data@gsk.com', 'privacy.officer@gsk.com', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- MANUFACTURING OPERATIONS DATA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC USE SCHEMA hls_manufacturing_ops;
# MAGIC
# MAGIC -- Insert Manufacturing Sites
# MAGIC INSERT INTO manufacturing_sites VALUES
# MAGIC ('SITE001', 'Wavre Manufacturing Plant', 'Wavre, Belgium', 'Biologics_Production', true, '2022-08-15', array('FDA_2023', 'EMA_2023'), array('Fill_Finish', 'Lyophilization', 'Vaccine_Production'), array('Bioreactors', 'Fill_Lines', 'Cold_Storage'), 50000000.0, 'wavre.manager@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('SITE002', 'Marietta Facility', 'Marietta, Pennsylvania, US', 'API_Production', true, '2021-11-20', array('FDA_2022', 'FDA_2024'), array('Chemical_Synthesis', 'Purification'), array('Reactors', 'HPLC_Systems', 'Crystallizers'), 25000000.0, 'marietta.manager@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('SITE003', 'Ware Distribution Center', 'Ware, UK', 'Distribution', true, '2023-03-10', array('MHRA_2023'), array('Cold_Chain_Storage', 'Secondary_Packaging'), array('Cold_Rooms', 'Packaging_Lines'), 100000000.0, 'ware.manager@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('SITE004', 'Singapore Biologics', 'Singapore', 'Biologics_Production', true, '2022-05-30', array('HSA_2022', 'FDA_2023'), array('Cell_Culture', 'Downstream_Processing'), array('Bioreactors', 'Chromatography_Systems'), 30000000.0, 'singapore.manager@gsk.com', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert Batch Genealogy
# MAGIC INSERT INTO batch_genealogy VALUES
# MAGIC ('BATCH001', 'VAC_RSV_001', 'SITE001', 'Line_A', '2024-01-15', 500000.0, 'Released', array(), array('BATCH001A', 'BATCH001B'), array('RM_001', 'RM_002'), 'Fill_Finish', 'GMP', '2024-01-30', '2026-01-30', current_timestamp(), current_timestamp()),
# MAGIC ('BATCH002', 'VAC_RSV_001', 'SITE001', 'Line_A', '2024-01-22', 500000.0, 'Released', array(), array('BATCH002A', 'BATCH002B'), array('RM_001', 'RM_003'), 'Fill_Finish', 'GMP', '2024-02-05', '2026-02-05', current_timestamp(), current_timestamp()),
# MAGIC ('BATCH003', 'API_HIV_001', 'SITE002', 'Reactor_3', '2024-01-10', 1000.0, 'Released', array(), array('BATCH003A'), array('RM_101', 'RM_102', 'RM_103'), 'API_Production', 'GMP', '2024-01-25', '2027-01-25', current_timestamp(), current_timestamp()),
# MAGIC ('BATCH004', 'VAC_MENING_001', 'SITE004', 'Line_B', '2024-01-18', 750000.0, 'In_Testing', array(), array(), array('RM_201', 'RM_202'), 'Fill_Finish', 'GMP', NULL, NULL, current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert Quality Control Results
# MAGIC INSERT INTO quality_control_results VALUES
# MAGIC ('QC001', 'BATCH001', 'Potency Assay', 'Release_Testing', 'ELISA', 90.0, 110.0, 98.5, 'Relative_Percent', 'PASS', '2024-01-28', 'analyst1@gsk.com', 'qc.manager@gsk.com', false, current_timestamp(), current_timestamp()),
# MAGIC ('QC002', 'BATCH001', 'Sterility Test', 'Release_Testing', 'USP <71>', NULL, NULL, 0.0, 'CFU', 'PASS', '2024-01-29', 'analyst2@gsk.com', 'qc.manager@gsk.com', false, current_timestamp(), current_timestamp()),
# MAGIC ('QC003', 'BATCH002', 'Potency Assay', 'Release_Testing', 'ELISA', 90.0, 110.0, 105.2, 'Relative_Percent', 'PASS', '2024-02-03', 'analyst1@gsk.com', 'qc.manager@gsk.com', false, current_timestamp(), current_timestamp()),
# MAGIC ('QC004', 'BATCH003', 'Purity HPLC', 'Release_Testing', 'HPLC-UV', 98.0, 102.0, 99.8, 'Percent', 'PASS', '2024-01-23', 'analyst3@gsk.com', 'qc.manager@gsk.com', false, current_timestamp(), current_timestamp()),
# MAGIC ('QC005', 'BATCH004', 'Endotoxin Test', 'In_Process', 'LAL', NULL, 5.0, 2.1, 'EU/mL', 'PASS', '2024-01-19', 'analyst2@gsk.com', 'qc.supervisor@gsk.com', false, current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert Supply Chain Inventory
# MAGIC INSERT INTO supply_chain_inventory VALUES
# MAGIC ('INV001', 'Raw_Material', 'RM_001', 'Excipient Buffer Solution pH 7.4', 'SITE001', 5000.0, 'Liters', 'LOT_RM001_2024', '2025-12-31', '2-8C', true, 'BioChem Supplies Ltd', current_timestamp(), current_timestamp()),
# MAGIC ('INV002', 'Raw_Material', 'RM_002', 'Adjuvant Component AS01', 'SITE001', 250.0, 'Kilograms', 'LOT_RM002_2024', '2026-06-30', '2-8C', true, 'Adjuvant Technologies', current_timestamp(), current_timestamp()),
# MAGIC ('INV003', 'Finished_Goods', 'VAC_RSV_001', 'RSV Vaccine Pre-filled Syringe', 'SITE003', 450000.0, 'Units', 'BATCH001', '2026-01-30', '2-8C', true, 'GSK Wavre', current_timestamp(), current_timestamp()),
# MAGIC ('INV004', 'WIP', 'API_HIV_001', 'HIV API Bulk Powder', 'SITE002', 800.0, 'Kilograms', 'BATCH003', '2027-01-25', '15-25C', false, 'GSK Marietta', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert Process Deviations
# MAGIC INSERT INTO process_deviations VALUES
# MAGIC ('DEV001', 'BATCH002', '2024-01-23', 'Environmental_Control', 'Minor', 'Temperature excursion in cold room for 15 minutes (9.5°C vs 2-8°C spec)', 'Temperature monitoring system delay in alert notification', 'Recalibrated temperature monitoring system, implemented redundant sensors', 'Updated temperature monitoring SOP to include more frequent manual checks', 'CAPA_2024_015', false, '2024-02-15', 'Closed', 'deviation.owner@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('DEV002', 'BATCH004', '2024-01-19', 'Equipment_Malfunction', 'Major', 'Fill pump stopped mid-batch, 20 vials potentially underfilled', 'Pump seal failure due to wear', 'Replaced pump seal, quarantined affected vials for weight check', 'Implemented preventive maintenance schedule for all fill pumps', 'CAPA_2024_018', true, NULL, 'Open', 'deviation.owner@gsk.com', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- COMMERCIAL ANALYTICS DATA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC USE SCHEMA hls_commercial_analytics;
# MAGIC
# MAGIC -- Insert Prescription Trends
# MAGIC INSERT INTO prescription_trends VALUES
# MAGIC ('RX001', 'Trelegy Ellipta', 'Respiratory', 'US', '2024-Q1', 1250000, 32.5, 8.5, 'IQVIA', 'Severe_Asthma', 'Pulmonology', 95, current_timestamp(), current_timestamp()),
# MAGIC ('RX002', 'Shingrix', 'Vaccines', 'US', '2024-Q1', 4500000, 89.2, 12.0, 'IQVIA', 'Adults_50plus', 'Primary_Care', 98, current_timestamp(), current_timestamp()),
# MAGIC ('RX003', 'Dovato', 'HIV', 'EU', '2024-Q1', 185000, 24.8, 15.3, 'IQVIA', 'Treatment_Experienced', 'Infectious_Disease', 92, current_timestamp(), current_timestamp()),
# MAGIC ('RX004', 'Nucala', 'Respiratory', 'Global', '2024-Q1', 425000, 18.6, 22.1, 'IQVIA', 'Severe_Asthma_Eosinophilic', 'Pulmonology', 94, current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert Market Access Contracts
# MAGIC INSERT INTO market_access_contracts VALUES
# MAGIC ('CTR001', 'UnitedHealth Group', 'PBM', 'Shingrix', 'US', 'Value_Based', '2024-01-01', '2026-12-31', 'Preferred_Tier2', 15.5, 5000000, true, 'Highly_Confidential', 'market.access@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('CTR002', 'NHS England', 'National_Payer', 'Nucala', 'UK', 'Outcomes_Based', '2023-07-01', '2026-06-30', 'Standard', 22.0, 50000, true, 'Highly_Confidential', 'market.access@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('CTR003', 'Aetna', 'Health_Plan', 'Trelegy Ellipta', 'US', 'Rebate', '2024-01-01', '2024-12-31', 'Preferred_Tier3', 18.0, 750000, false, 'Confidential', 'market.access@gsk.com', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert HCP Engagement
# MAGIC INSERT INTO hcp_engagement VALUES
# MAGIC ('ENG001', 'HCP_12345', 'Pulmonology', 'In_Person_Detail', '2024-01-15', 'Trelegy Ellipta', 'Respiratory', 'Field_Sales', 25, 8, true, 'REP_567', true, current_timestamp(), current_timestamp()),
# MAGIC ('ENG002', 'HCP_23456', 'Primary_Care', 'Virtual_Meeting', '2024-01-16', 'Shingrix', 'Vaccines', 'Virtual', 15, 9, false, 'REP_789', true, current_timestamp(), current_timestamp()),
# MAGIC ('ENG003', 'HCP_34567', 'Infectious_Disease', 'Speaker_Program', '2024-01-18', 'Dovato', 'HIV', 'Events', 120, 10, true, 'REP_234', true, current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert Competitive Intelligence
# MAGIC INSERT INTO competitive_intelligence VALUES
# MAGIC ('INT001', 'Pfizer', 'Paxlovid', 'Infectious Diseases', 'Product_Launch', 'New indication approved for immunocompromised patients', 'Regulatory_Filing', 'US', 'High', 'Monitor_Impact', true, 'competitive.intel@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('INT002', 'Moderna', 'mRNA RSV Vaccine', 'Vaccines', 'Clinical_Trial', 'Phase III trial results show 85% efficacy in older adults', 'Clinical_Data', 'Global', 'High', 'Significant', true, 'competitive.intel@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('INT003', 'AstraZeneca', 'Tezspire', 'Respiratory', 'Market_Share', 'Gaining market share in severe asthma segment', 'Market_Data', 'EU', 'Medium', 'Moderate', false, 'competitive.intel@gsk.com', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- INFRASTRUCTURE DATA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC USE SCHEMA hls_infrastructure;
# MAGIC
# MAGIC -- Insert Source Systems Catalog
# MAGIC INSERT INTO source_systems_catalog VALUES
# MAGIC ('SYS001', 'Medidata Rave EDC', 'Clinical_Trial_Management', 'Medidata Solutions', 'Clinical Research', 'clinical.systems@gsk.com', 'clinical.tech@gsk.com', 'API', 'Real_Time', 99.9, 'Tier_1_Critical', true, 'Production', 'api.mdsol.com/rave', current_timestamp(), current_timestamp()),
# MAGIC ('SYS002', 'SAP MES', 'Manufacturing_Execution', 'SAP', 'Manufacturing', 'manufacturing.systems@gsk.com', 'manufacturing.tech@gsk.com', 'Database_Replication', 'Real_Time', 99.95, 'Tier_1_Critical', true, 'Production', 'sap-mes.gsk.internal', current_timestamp(), current_timestamp()),
# MAGIC ('SYS003', 'LabWare LIMS', 'Laboratory_Management', 'LabWare', 'Quality Control', 'quality.systems@gsk.com', 'quality.tech@gsk.com', 'API', 'Hourly', 99.5, 'Tier_2_Important', true, 'Production', 'lims-api.gsk.internal', current_timestamp(), current_timestamp()),
# MAGIC ('SYS004', 'Veeva CRM', 'Sales_Force_Automation', 'Veeva Systems', 'Commercial', 'commercial.systems@gsk.com', 'commercial.tech@gsk.com', 'API', 'Daily', 99.0, 'Tier_2_Important', false, 'Production', 'api.veevavault.com', current_timestamp(), current_timestamp()),
# MAGIC ('SYS005', 'Argus Safety', 'Pharmacovigilance', 'Oracle', 'Drug Safety', 'safety.systems@gsk.com', 'safety.tech@gsk.com', 'Database_Replication', 'Real_Time', 99.99, 'Tier_1_Critical', true, 'Production', 'argus.gsk.internal', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert Data Pipelines
# MAGIC INSERT INTO data_pipelines VALUES
# MAGIC ('PIPE001', 'clinical_trials_ingestion', 'Batch_Ingestion', 'Medidata Rave EDC', 'clinical_research', 'clinical_trials', 'Databricks_Workflows', '0 */4 * * *', 'Gold', 'Medallion architecture with quality checks', 428, '2024-01-20 14:00:00', 'Success', 'data.engineering@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('PIPE002', 'batch_genealogy_realtime', 'Streaming_Ingestion', 'SAP MES', 'manufacturing_ops', 'batch_genealogy', 'Databricks_Workflows', 'Continuous', 'Gold', 'Real-time CDC with Delta CDF', 1247, '2024-01-20 15:30:00', 'Success', 'data.engineering@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('PIPE003', 'genomics_processing', 'Batch_Processing', 'Illumina BaseSpace', 'clinical_research', 'genomics_datasets', 'Databricks_Workflows', '0 2 * * 0', 'Silver', 'Bioinformatics pipeline with variant calling', 4, '2024-01-14 02:00:00', 'Success', 'bioinformatics@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('PIPE004', 'prescription_trends_aggregation', 'Batch_Ingestion', 'IQVIA', 'commercial_analytics', 'prescription_trends', 'Databricks_Workflows', '0 6 * * 1', 'Gold', 'Weekly aggregation with trend calculations', 156, '2024-01-15 06:00:00', 'Success', 'data.engineering@gsk.com', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert Data Quality Metrics
# MAGIC INSERT INTO data_quality_metrics VALUES
# MAGIC ('DQ001', 'clinical_research.clinical_trials', 'Completeness', 'enrollment_actual_populated', 98.5, 95.0, 100.0, 'PASS', '2024-01-20 08:00:00', 'Completeness', 0, NULL, current_timestamp()),
# MAGIC ('DQ002', 'clinical_research.clinical_trials', 'Validity', 'valid_email_format', 100.0, 100.0, 100.0, 'PASS', '2024-01-20 08:00:00', 'Validity', 0, NULL, current_timestamp()),
# MAGIC ('DQ003', 'manufacturing_ops.batch_genealogy', 'Timeliness', 'data_freshness_minutes', 5.2, 0.0, 15.0, 'PASS', '2024-01-20 15:30:00', 'Timeliness', 0, NULL, current_timestamp()),
# MAGIC ('DQ004', 'commercial_analytics.prescription_trends', 'Accuracy', 'market_share_sum_check', 99.8, 98.0, 100.2, 'PASS', '2024-01-15 06:30:00', 'Accuracy', 0, NULL, current_timestamp()),
# MAGIC ('DQ005', 'patient_privacy.consent_records', 'Consistency', 'consent_withdrawal_logic', 95.0, 99.0, 100.0, 'FAIL', '2024-01-20 09:00:00', 'Consistency', 12, 'Found 12 records with withdrawal_date but active status', current_timestamp());
# MAGIC
# MAGIC -- Insert Integration Interfaces
# MAGIC INSERT INTO integration_interfaces VALUES
# MAGIC ('API001', 'Medidata Rave API', 'REST_API', 'Medidata Rave EDC', 'Databricks', 'HTTPS', 'https://api.mdsol.com/rave/v1', 'OAuth2', 'JSON', 50000, 2000, 'integration.team@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('API002', 'SAP MES Database Link', 'Database_Connection', 'SAP MES', 'Databricks', 'JDBC', 'jdbc:sap://sap-mes.gsk.internal:30015', 'Service_Account', 'Table', 500000, 500, 'integration.team@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('API003', 'Veeva Vault API', 'REST_API', 'Veeva CRM', 'Databricks', 'HTTPS', 'https://api.veevavault.com/v21.3', 'Session_Token', 'JSON', 25000, 3000, 'integration.team@gsk.com', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- GOVERNANCE DATA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC USE SCHEMA hls_governance;
# MAGIC
# MAGIC -- Insert Data Ownership Registry
# MAGIC INSERT INTO data_ownership_registry VALUES
# MAGIC ('OWN001', 'clinical_research.clinical_trials', 'Table', 'Clinical Research', 'clinical.data.owner@gsk.com', 'clinical.tech.steward@gsk.com', 'oncology.steward@gsk.com', 'clinical.trial.sme@gsk.com', 'Enterprise_Strategic', 'Managed', '2023-01-01', 'Quarterly', '2024-01-10', current_timestamp(), current_timestamp()),
# MAGIC ('OWN002', 'manufacturing_ops.batch_genealogy', 'Table', 'Manufacturing', 'manufacturing.data@gsk.com', 'manufacturing.tech.steward@gsk.com', 'quality.steward@gsk.com', 'batch.records.sme@gsk.com', 'Enterprise_Strategic', 'Optimized', '2022-06-01', 'Quarterly', '2024-01-15', current_timestamp(), current_timestamp()),
# MAGIC ('OWN003', 'clinical_research.genomics_datasets', 'Table', 'Clinical Research', 'genomics.data@gsk.com', 'bioinformatics.tech@gsk.com', 'genomics.steward@gsk.com', 'genomics.scientist@gsk.com', 'Domain_Critical', 'Managed', '2023-03-01', 'Semi_Annual', '2023-09-01', current_timestamp(), current_timestamp()),
# MAGIC ('OWN004', 'commercial_analytics.prescription_trends', 'Table', 'Commercial', 'commercial.analytics@gsk.com', 'commercial.tech@gsk.com', 'sales.analytics.steward@gsk.com', 'market.insights.sme@gsk.com', 'Domain_Critical', 'Defined', '2023-07-01', 'Semi_Annual', '2024-01-05', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert Data Classification
# MAGIC INSERT INTO data_classification VALUES
# MAGIC ('CLS001', 'clinical_research.clinical_trials', 'Confidential', 'Critical', 'Clinical_Trial_Data', array('FDA', 'EMA', 'ICH_GCP'), true, 'Role_Based_MFA', 'Clinical_Trial_25yr', '2023-02-01', 'data.governance@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('CLS002', 'clinical_research.genomics_datasets', 'Restricted', 'Critical', 'Special_Category_Genetic', array('GDPR', 'HIPAA'), true, 'Attribute_Based_Access', 'Genetic_Data_Indefinite', '2023-03-15', 'data.governance@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('CLS003', 'manufacturing_ops.batch_genealogy', 'Restricted', 'Critical', 'GMP_Manufacturing', array('FDA_21CFR211', 'EMA_GMP'), true, 'Role_Based_MFA', 'GMP_Records_Expiry_Plus_1yr', '2022-06-20', 'data.governance@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('CLS004', 'commercial_analytics.market_access_contracts', 'Restricted', 'High', 'Commercial_Confidential', array(), true, 'Need_To_Know_Basis', 'Contract_End_Plus_7yr', '2023-08-01', 'data.governance@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('CLS005', 'reference_data.therapeutic_areas', 'Public', 'Low', 'Reference_Data', array(), false, 'Public_Access', 'Indefinite', '2023-01-01', 'data.governance@gsk.com', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert Governance Policies
# MAGIC INSERT INTO governance_policies VALUES
# MAGIC ('POL001', 'Clinical Data Retention Policy', 'Data_Retention', 'Defines retention periods for clinical trial data per ICH GCP E6(R2) requirements', 'chief.data.officer@gsk.com', '2023-01-15', '2023-02-01', '2026-02-01', 'Active', array('Clinical Research', 'Regulatory'), true, 'https://policies.gsk.com/POL-RET-001', current_timestamp(), current_timestamp()),
# MAGIC ('POL002', 'Patient Privacy and Consent Policy', 'Privacy', 'Governs collection, use, and management of patient consent and privacy rights', 'privacy.officer@gsk.com', '2023-03-01', '2023-04-01', '2025-04-01', 'Active', array('Clinical Research', 'Patient Privacy'), true, 'https://policies.gsk.com/POL-PRIV-001', current_timestamp(), current_timestamp()),
# MAGIC ('POL003', 'GxP Data Integrity Policy', 'Data_Quality', 'Ensures ALCOA+ principles for GxP-regulated data systems', 'quality.officer@gsk.com', '2022-06-01', '2022-07-01', '2025-07-01', 'Active', array('Manufacturing', 'Regulatory'), true, 'https://policies.gsk.com/POL-DQ-001', current_timestamp(), current_timestamp()),
# MAGIC ('POL004', 'Data Classification and Handling Policy', 'Data_Governance', 'Establishes data classification levels and handling requirements', 'chief.data.officer@gsk.com', '2023-01-01', '2023-02-01', '2025-02-01', 'Active', array('All'), true, 'https://policies.gsk.com/POL-GOV-001', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- AI/ML ANALYTICS DATA
# MAGIC -- ============================================================================
# MAGIC
# MAGIC USE SCHEMA hls_ai_ml_analytics;
# MAGIC
# MAGIC -- Insert ML Models Registry
# MAGIC INSERT INTO ml_models_registry VALUES
# MAGIC ('MODEL001', 'Adverse Event Prediction Model', 'AE_Prediction', 'Classification', 'XGBoost', 'Production', 'v2.1.0', 'TRAIN001', 'AUROC', 0.87, 'High', 'Completed', '2024-01-05', 'mlflow-run-abc123', 'ai.center@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('MODEL002', 'Clinical Trial Patient Matching', 'Trial_Optimization', 'Recommendation', 'Neural_Network', 'Validation', 'v1.3.0', 'TRAIN002', 'Precision@10', 0.92, 'Medium', 'In_Progress', NULL, 'mlflow-run-def456', 'ai.center@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('MODEL003', 'Batch Quality Predictor', 'Predictive_Maintenance', 'Regression', 'Random_Forest', 'Production', 'v3.0.1', 'TRAIN003', 'RMSE', 2.1, 'High', 'Completed', '2023-11-20', 'mlflow-run-ghi789', 'manufacturing.ai@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('MODEL004', 'Drug Discovery Target Identification', 'Drug_Discovery', 'Multi_Label_Classification', 'Transformer', 'Development', 'v0.8.0', 'TRAIN004', 'F1_Score', 0.78, 'Low', 'Not_Started', NULL, 'mlflow-run-jkl012', 'research.ai@gsk.com', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert Feature Stores
# MAGIC INSERT INTO feature_stores VALUES
# MAGIC ('FEAT001', 'patient_demographics_features', 'Patient_Features', 'Categorical', 'clinical_research.patient_cohorts', 'Age binning, gender encoding, ethnicity encoding', 0.85, array('MODEL001', 'MODEL002'), 95, 'Daily', 'ai.center@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('FEAT002', 'clinical_endpoint_aggregates', 'Clinical_Features', 'Numerical', 'clinical_research.clinical_endpoints', 'Rolling averages, trend indicators, variance metrics', 0.92, array('MODEL002'), 98, 'Weekly', 'ai.center@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('FEAT003', 'batch_process_parameters', 'Manufacturing_Features', 'Numerical', 'manufacturing_ops.batch_genealogy', 'Process deviation counts, temperature profiles, yield metrics', 0.88, array('MODEL003'), 97, 'Real_Time', 'manufacturing.ai@gsk.com', current_timestamp(), current_timestamp()),
# MAGIC ('FEAT004', 'molecular_descriptors', 'Molecular_Features', 'Numerical', 'clinical_research.genomics_datasets', 'Physicochemical properties, structural fingerprints, binding affinities', 0.76, array('MODEL004'), 90, 'On_Demand', 'research.ai@gsk.com', current_timestamp(), current_timestamp());
# MAGIC
# MAGIC -- Insert ML Training Datasets
# MAGIC INSERT INTO ml_training_datasets VALUES
# MAGIC ('TRAIN001', 'Adverse Event Training Dataset 2023', array('clinical_research.adverse_events', 'clinical_research.patient_cohorts'), 'AE_Prediction', 'Multiple', 125000, 45, 'serious_ae_flag', 'Stratified_80_20_Split', true, 'Anonymized', true, 'v2.0', 'ai.center@gsk.com', '2023-12-01', current_timestamp()),
# MAGIC ('TRAIN002', 'Patient Trial Matching Dataset', array('clinical_research.patient_cohorts', 'clinical_research.clinical_trials'), 'Trial_Optimization', 'Oncology', 85000, 32, 'matched_trial_id', 'Temporal_Split_2023', true, 'Pseudonymized', true, 'v1.5', 'ai.center@gsk.com', '2024-01-10', current_timestamp()),
# MAGIC ('TRAIN003', 'Manufacturing Quality Dataset', array('manufacturing_ops.batch_genealogy', 'manufacturing_ops.quality_control_results'), 'Predictive_Maintenance', 'Vaccines', 45000, 28, 'batch_quality_score', 'Random_70_30_Split', false, 'Non_Personal', false, 'v3.1', 'manufacturing.ai@gsk.com', '2023-10-15', current_timestamp()),
# MAGIC ('TRAIN004', 'Drug Target Genomics Dataset', array('clinical_research.genomics_datasets'), 'Drug_Discovery', 'Multiple', 5000, 1250, 'target_binding_affinity', 'K_Fold_Cross_Validation', true, 'Anonymized', true, 'v0.9', 'research.ai@gsk.com', '2024-01-18', current_timestamp());
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- COMPLETION MESSAGE
# MAGIC -- ============================================================================
# MAGIC
# MAGIC SELECT 'GSK Enterprise Catalog - Data Generation Complete!' AS status,
# MAGIC        'Total Catalogs: 1' AS catalogs,
# MAGIC        'Total Schemas: 9' AS schemas,
# MAGIC        'Total Tables: 32' AS tables,
# MAGIC        'Sample Records Inserted: 200+' AS records;
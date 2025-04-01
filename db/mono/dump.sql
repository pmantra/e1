-- MySQL dump 10.13  Distrib 8.0.22, for osx10.15 (x86_64)
--
-- Host: 127.0.0.1    Database: maven
-- ------------------------------------------------------
-- Server version	5.6.42-google-log

CREATE DATABASE IF NOT EXISTS `maven`;
USE `maven`;

/*!40101 SET @OLD_CHARACTER_SET_CLIENT = @@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS = @@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION = @@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE = @@TIME_ZONE */;
/*!40103 SET TIME_ZONE = '+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS = @@UNIQUE_CHECKS, UNIQUE_CHECKS = 0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS = @@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS = 0 */;
/*!40101 SET @OLD_SQL_MODE = @@SQL_MODE, SQL_MODE = 'NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES = @@SQL_NOTES, SQL_NOTES = 0 */;
SET @MYSQLDUMP_TEMP_LOG_BIN = @@SESSION.SQL_LOG_BIN;
SET @@SESSION.SQL_LOG_BIN = 0;

--
-- Table structure for table `organization`
--

DROP TABLE IF EXISTS `organization`;
/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `organization`
(
    `id`                     int(11)                                                                         NOT NULL AUTO_INCREMENT,
    `name`                   varchar(50) COLLATE utf8mb4_unicode_ci                                          NOT NULL,
    `display_name`           varchar(50) COLLATE utf8mb4_unicode_ci                                                   DEFAULT NULL,
    `internal_summary`       text COLLATE utf8mb4_unicode_ci,
    `vertical_group_version` varchar(120) COLLATE utf8mb4_unicode_ci                                                  DEFAULT NULL,
    `message_price`          int(11)                                                                                  DEFAULT NULL,
    `directory_name`         varchar(120) COLLATE utf8mb4_unicode_ci                                                  DEFAULT NULL,
    `activated_at`           datetime                                                                                 DEFAULT NULL,
    `terminated_at`           datetime                                                                                 DEFAULT NULL,
    `medical_plan_only`      tinyint(1)                                                                      NOT NULL DEFAULT '0',
    `employee_only`          tinyint(1)                                                                      NOT NULL DEFAULT '0',
    `bms_enabled`            tinyint(1)                                                                      NOT NULL DEFAULT '0',
    `rx_enabled`             tinyint(1)                                                                      NOT NULL DEFAULT '1',
    `icon`                   varchar(2048) COLLATE utf8mb4_unicode_ci                                                 DEFAULT NULL,
    `json`                   text COLLATE utf8mb4_unicode_ci,
    `session_ttl`            int(11)                                                                                  DEFAULT NULL,
    `multitrack_enabled`     tinyint(1)                                                                      NOT NULL,
    `internal_type`          enum ('REAL','TEST','DEMO_OR_VIP','MAVEN_FOR_MAVEN') COLLATE utf8mb4_unicode_ci NOT NULL,
    `education_only`         tinyint(1)                                                                      NOT NULL,
    `alegeus_employer_id`    varchar(12) COLLATE utf8mb4_unicode_ci                                                   DEFAULT NULL,
    `data_provider`          tinyint(1)                                                                      NOT NULL DEFAULT '0',
    `eligibility_type` enum('STANDARD','ALTERNATE','FILELESS','CLIENT_SPECIFIC','SAML','HEALTHPLAN','UNKNOWN') COLLATE utf8mb4_unicode_ci DEFAULT NULL,


    PRIMARY KEY (`id`),
    UNIQUE KEY `alegeus_employer_id` (`alegeus_employer_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `organization_email_domain`
--

DROP TABLE IF EXISTS `organization_email_domain`;
/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `organization_email_domain`
(
    `id`                int(11)                                                        NOT NULL AUTO_INCREMENT,
    `domain`            varchar(120) COLLATE utf8mb4_unicode_ci                        NOT NULL,
    `organization_id`   int(11)                                                        NOT NULL,
    `eligibility_logic` enum ('CLIENT_SPECIFIC','FILELESS') COLLATE utf8mb4_unicode_ci NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `domain` (`domain`),
    KEY `organization_id` (`organization_id`),
    CONSTRAINT `organization_email_domain_ibfk_1` FOREIGN KEY (`organization_id`) REFERENCES `organization` (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `organization_external_id`
--

DROP TABLE IF EXISTS `organization_external_id`;
/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `organization_external_id`
(
    `id`              int(11)                                                                     NOT NULL AUTO_INCREMENT,
    `idp`             enum ('VIRGIN_PULSE','OKTA','CASTLIGHT','OPTUM') COLLATE utf8mb4_unicode_ci DEFAULT NULL,
    `external_id`     varchar(120) COLLATE utf8mb4_unicode_ci                                     NOT NULL,
    `organization_id` int(11) DEFAULT NULL,
    `data_provider_organization_id` int(11) DEFAULT NULL,
    `identity_provider_id` int(11) DEFAULT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `idp_external_id` (`idp`, `external_id`),
    KEY `organization_id` (`organization_id`),
    CONSTRAINT `organization_external_id_ibfk_1` FOREIGN KEY (`organization_id`) REFERENCES `organization` (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `client_track`
--

DROP TABLE IF EXISTS `client_track`;
/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `client_track`
(
    `id`              int auto_increment PRIMARY KEY,
    `track`           varchar(120)         not null,
    `extension_id`    int                  null,
    `organization_id` int                  not null,
    `created_at`      datetime             null,
    `modified_at`     datetime             null,
    `active`          tinyint(1) default 1 not null,
    `launch_date`     date                 null,
    `length_in_days`  int                  not null,
    `ended_at`        datetime             null,
    CONSTRAINT `uc_client_track_organization_track`
        UNIQUE (`organization_id`, `track`, `length_in_days`, `active`)
#     CONSTRAINT `client_track_ibfk_1`
#         FOREIGN KEY (`extension_id`) REFERENCES `track_extension` (`id`),
#     CONSTRAINT `client_track_ibfk_2`
#         FOREIGN KEY (`organization_id`) REFERENCES `organization` (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `reimbursement_organization_settings`
--

DROP TABLE IF EXISTS `reimbursement_organization_settings`;
/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `reimbursement_organization_settings`
(
    `id`                              bigint PRIMARY KEY,
    `organization_id`                 int                                                                             not null,
    `name`                            varchar(50)                                                                     null,
    `benefit_overview_resource_id`    int                                                                             null,
    `benefit_faq_resource_id`         int                                                                             not null,
    `survey_url`                      varchar(255)                                                                    not null,
    `required_module_id`              int                                                                             null,
    `started_at`                      datetime                                                                        null,
    `ended_at`                        datetime                                                                        null,
    `required_track`                  varchar(120)                                                                    null,
    `taxation_status`                 enum ('TAXABLE', 'NON_TAXABLE', 'ADOPTION_QUALIFIED', 'ADOPTION_NON_QUALIFIED') null,
    `debit_card_enabled`              tinyint(1)                                                                      not null,
    `created_at`                      datetime                                                                        null,
    `modified_at`                     datetime                                                                        null,
    `cycles_enabled`                  tinyint(1)                                                                      not null,
    `direct_payment_enabled`          tinyint(1)                                                                      not null,
    `rx_direct_payment_enabled`       tinyint(1)                                                                      not null,
    `deductible_accumulation_enabled` tinyint(1)                                                                      not null,
    `closed_network`                  tinyint(1)                                                                      not null,
    `fertility_program_type`          enum ('CARVE_OUT', 'WRAP_AROUND')                                               not null,
    `fertility_requires_diagnosis`    tinyint(1)                                                                      not null,
    `fertility_allows_taxable`        tinyint(1)                                                                      not null,
    `payments_customer_id`            char(36)                                                                        null
#     CONSTRAINT `reimbursement_organization_settings_ibfk_1`
#         FOREIGN KEY (`benefit_overview_resource_id`) REFERENCES `resource` (`id`),
#     CONSTRAINT `reimbursement_organization_settings_ibfk_2`
#         FOREIGN KEY (`benefit_faq_resource_id`) REFERENCES `resource` (`id`),
#     CONSTRAINT `reimbursement_organization_settings_ibfk_3`
#         FOREIGN KEY (`required_module_id`) REFERENCES `module` (`id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `credit`
--

DROP TABLE IF EXISTS `credit`;
/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `credit` (
  `modified_at` datetime DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NOT NULL,
  `amount` double(8,2) NULL,
  `activated_at` datetime DEFAULT NULL,
  `used_at` datetime DEFAULT NULL,
  `expires_at` datetime DEFAULT NULL,
  `appointment_id` int(11) DEFAULT NULL,
  `referral_code_use_id` int(11) DEFAULT NULL,
  `message_billing_id` int(11) DEFAULT NULL,
  `organization_package_id` int(11) DEFAULT NULL,
  `organization_employee_id` int(11) DEFAULT NULL,
  `json` text,
  `eligibility_member_id_deleted` int(11) DEFAULT NULL,
  `eligibility_verification_id` int(11) DEFAULT NULL,
  `eligibility_member_id` bigint(20) DEFAULT NULL,
  `eligibility_member_2_id` bigint(20) DEFAULT NULL,
  `eligibility_member_2_version` int(11) DEFAULT NULL,
  `eligibility_verification_2_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `user_id` (`user_id`),
  KEY `organization_employee_id` (`organization_employee_id`),
  KEY `eligibility_member_id` (`eligibility_member_id_deleted`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `organization_employee`
--
DROP TABLE IF EXISTS `organization_employee`;
/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `organization_employee` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `organization_id` int(11) NOT NULL,
  `unique_corp_id` varchar(120) DEFAULT NULL,
  `email` varchar(120) DEFAULT NULL,
  `date_of_birth` date NOT NULL,
  `first_name` varchar(40) DEFAULT NULL,
  `last_name` varchar(40) DEFAULT NULL,
  `work_state` varchar(32) DEFAULT NULL,
  `modified_at` datetime DEFAULT NULL,
  `deleted_at` datetime DEFAULT NULL,
  `retention_start_date` date DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `json` text,
  `dependent_id` varchar(120) NOT NULL DEFAULT '',
  `eligibility_member_id_deleted` int(11) DEFAULT NULL,
  `alegeus_id` varchar(30) DEFAULT NULL,
  `eligibility_member_id` bigint(20) DEFAULT NULL,
  `eligibility_member_2_id` bigint(20) DEFAULT NULL,
  `eligibility_member_2_version` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB 
  DEFAULT CHARSET=utf8mb4;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `member_track`
--
DROP TABLE IF EXISTS `member_track`;
/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `member_track` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `client_track_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `organization_employee_id` int(11) NOT NULL,
  `auto_transitioned` tinyint(1) NOT NULL,
  `created_at` datetime DEFAULT NULL,
  `modified_at` datetime DEFAULT NULL,
  `is_employee` tinyint(1) DEFAULT NULL,
  `ended_at` datetime DEFAULT NULL,
  `legacy_program_id` int(11) DEFAULT NULL,
  `legacy_module_id` int(11) DEFAULT NULL,
  `name` varchar(120) COLLATE utf8mb4_unicode_ci NOT NULL,
  `transitioning_to` varchar(120) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `anchor_date` date DEFAULT NULL,
  `previous_member_track_id` int(11) DEFAULT NULL,
  `bucket_id` varchar(36) COLLATE utf8mb4_unicode_ci NOT NULL,
  `track_extension_id` int(11) DEFAULT NULL,
  `closure_reason_id` int(11) DEFAULT NULL,
  `start_date` date NOT NULL,
  `activated_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `eligibility_member_id_deleted` int(11) DEFAULT NULL,
  `eligibility_verification_id` int(11) DEFAULT NULL,
  `qualified_for_optout` tinyint(1) DEFAULT NULL,
  `sub_population_id` bigint(20) DEFAULT NULL,
  `modified_by` varchar(120) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `change_reason` varchar(120) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `eligibility_member_id` bigint(20) DEFAULT NULL,
  `eligibility_member_2_id` bigint(20) DEFAULT NULL,
  `eligibility_member_2_version` int(11) DEFAULT NULL,
  `eligibility_verification_2_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;



--
-- Table structure for table `organization_employee_dependent`
--
DROP TABLE IF EXISTS `organization_employee_dependent`;
/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `organization_employee_dependent` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `organization_employee_id` int(11) DEFAULT NULL,
  `first_name` varchar(40) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `last_name` varchar(40) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `middle_name` varchar(40) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `modified_at` datetime DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `alegeus_dependent_id` varchar(30) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `reimbursement_wallet_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `reimbursement_wallet`
--
DROP TABLE IF EXISTS `reimbursement_wallet`;
/*!40101 SET @saved_cs_client = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `reimbursement_wallet` (
  `id` bigint(20) NOT NULL,
  `user_id` int(11) DEFAULT NULL,
  `reimbursement_organization_settings_id` bigint(20) NOT NULL,
  `organization_employee_id` int(11) DEFAULT NULL,
  `state` enum('PENDING','QUALIFIED','DISQUALIFIED','EXPIRED','RUNOUT') COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'PENDING',
  `note` varchar(4096) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `reimbursement_method` enum('DIRECT_DEPOSIT','PAYROLL','MMB_DIRECT_PAYMENT') COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `taxation_status` enum('TAXABLE','NON_TAXABLE','ADOPTION_QUALIFIED','ADOPTION_NON_QUALIFIED') COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `reimbursement_wallet_debit_card_id` bigint(20) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `modified_at` datetime DEFAULT NULL,
  `primary_expense_type` enum('FERTILITY','ADOPTION','EGG_FREEZING','SURROGACY','CHILDCARE','MATERNITY','MENOPAUSE','PRECONCEPTION_WELLNESS','DONOR','PRESERVATION') COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `payments_customer_id` char(36) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `alegeus_id` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  `initial_eligibility_member_id_deleted` int(11) DEFAULT NULL,
  `initial_eligibility_verification_id` int(11) DEFAULT NULL,
  `initial_eligibility_member_id` bigint(20) DEFAULT NULL,
  `initial_eligibility_member_2_id` bigint(20) DEFAULT NULL,
  `initial_eligibility_member_2_version` int(11) DEFAULT NULL,
  `initial_eligibility_verification_2_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
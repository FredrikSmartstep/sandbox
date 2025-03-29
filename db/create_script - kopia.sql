-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema documents_db_2
-- -----------------------------------------------------
DROP SCHEMA IF EXISTS `documents_db_2` ;

-- -----------------------------------------------------
-- Schema documents_db_2
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `documents_db_2` DEFAULT CHARACTER SET utf8 COLLATE utf8_swedish_ci ;
USE `documents_db_2` ;

-- -----------------------------------------------------
-- Table `documents_db_2`.`company`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`company` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(100) NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `name_UNIQUE` (`name` ASC) VISIBLE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`HTA_agency`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`HTA_agency` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(45) NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `name_UNIQUE` (`name` ASC) VISIBLE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`HTA_document`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`HTA_document` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `title` VARCHAR(100) NULL,
  `diarie_nr` VARCHAR(45) NULL,
  `application_type` VARCHAR(150) NULL,
  `date` DATE NULL,
  `decision` VARCHAR(45) NULL,
  `document_type` VARCHAR(45) NULL,
  `idcompany` INT NULL,
  `idHTA_agency` INT NOT NULL,
  `decision_summary` TEXT NULL,
  `efficacy_summary` TEXT NULL,
  `safety_summary` TEXT NULL,
  `limitations` VARCHAR(200) NULL,
  `three_part_deal` TINYINT(1) NULL,
  `annual_turnover` VARCHAR(45) NULL,
  `previously_licensed_medicine` TINYINT(1) NULL,
  `biosim` TINYINT(1) NULL,
  `resubmission` TINYINT(1) NULL,
  `changed_decision` TINYINT(1) NULL,
  `new_indication` TINYINT(1) NULL,
  `new_form` TINYINT(1) NULL,
  `new_price` TINYINT(1) NULL,
  `new_strength` TINYINT(1) NULL,
  `removed` TINYINT(1) NULL,
  `temporary` TINYINT(1) NULL,
  `sv_indications` VARCHAR(1000) NULL,
  `currency` VARCHAR(45) NULL,
  `requested_complement` TINYINT(1) NULL,
  `requested_information` VARCHAR(200) NULL,
  `requested_complement_submitted` TINYINT(1) NULL,
  `latest_decision_date` DATE NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_HTA_document_company_idx` (`idcompany` ASC) VISIBLE,
  INDEX `fk_HTA_document_HTA_agency1_idx` (`idHTA_agency` ASC) VISIBLE,
  UNIQUE INDEX `unique_date_diarie_document_type` (`diarie_nr` ASC, `date` ASC, `document_type` ASC) VISIBLE,
  CONSTRAINT `fk_HTA_document_company`
    FOREIGN KEY (`idcompany`)
    REFERENCES `documents_db_2`.`company` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_HTA_document_HTA_agency1`
    FOREIGN KEY (`idHTA_agency`)
    REFERENCES `documents_db_2`.`HTA_agency` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`product`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`product` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(100) NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `name_UNIQUE` (`name` ASC) VISIBLE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`HTA_document_product`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`HTA_document_product` (
  `idHTA_document` INT NOT NULL,
  `idproduct` INT NOT NULL,
  PRIMARY KEY (`idHTA_document`, `idproduct`),
  INDEX `fk_HTA_document_has_product_product1_idx` (`idproduct` ASC) VISIBLE,
  INDEX `fk_HTA_document_has_product_HTA_document1_idx` (`idHTA_document` ASC) VISIBLE,
  CONSTRAINT `fk_HTA_document_has_product_HTA_document1`
    FOREIGN KEY (`idHTA_document`)
    REFERENCES `documents_db_2`.`HTA_document` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_HTA_document_has_product_product1`
    FOREIGN KEY (`idproduct`)
    REFERENCES `documents_db_2`.`product` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`indication`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`indication` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `who_full_desc` VARCHAR(300) NULL,
  `icd10_code` VARCHAR(45) NULL,
  `icd10_3_code` VARCHAR(45) NULL,
  `icd10_3_code_desc` VARCHAR(200) NULL,
  `valid_icd10_clinicaluse` TINYINT NULL,
  `valid_icd10_primary` TINYINT NULL,
  `valid_icd10_asterisk` TINYINT NULL,
  `valid_icd10_dagger` TINYINT NULL,
  `valid_icd10_sequelae` TINYINT NULL,
  `age_range` VARCHAR(45) NULL,
  `gender` VARCHAR(3) NULL,
  `status` VARCHAR(3) NULL,
  `who_start_date` DATE NULL,
  `who_end_date` DATE NULL,
  `who_revision_history` VARCHAR(3) NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `ICD_UNIQUE` (`icd10_code` ASC) VISIBLE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`product_has_indication`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`product_has_indication` (
  `idproduct` INT NOT NULL,
  `idindication` INT NOT NULL,
  `source` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`idproduct`, `idindication`, `source`),
  INDEX `fk_product_has_indication_indication1_idx` (`idindication` ASC) VISIBLE,
  INDEX `fk_product_has_indication_product1_idx` (`idproduct` ASC) VISIBLE,
  CONSTRAINT `fk_product_has_indication_product1`
    FOREIGN KEY (`idproduct`)
    REFERENCES `documents_db_2`.`product` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_product_has_indication_indication1`
    FOREIGN KEY (`idindication`)
    REFERENCES `documents_db_2`.`indication` (`id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`staff`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`staff` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `profession` VARCHAR(45) NOT NULL,
  `name` VARCHAR(45) NOT NULL,
  `idHTA_agency` INT NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_personal_HTA_agency1_idx` (`idHTA_agency` ASC) VISIBLE,
  UNIQUE INDEX `unique_name_profession` (`profession` ASC, `name` ASC) VISIBLE,
  CONSTRAINT `fk_personal_HTA_agency1`
    FOREIGN KEY (`idHTA_agency`)
    REFERENCES `documents_db_2`.`HTA_agency` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`HTA_document_staff`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`HTA_document_staff` (
  `idHTA_document` INT NOT NULL,
  `idstaff` INT NOT NULL,
  `role` VARCHAR(45) NULL,
  `dissent` VARCHAR(45) NULL,
  PRIMARY KEY (`idHTA_document`, `idstaff`),
  INDEX `fk_HTA_document_has_personal_personal1_idx` (`idstaff` ASC) VISIBLE,
  INDEX `fk_HTA_document_has_personal_HTA_document1_idx` (`idHTA_document` ASC) VISIBLE,
  UNIQUE INDEX `role_UNIQUE` (`role` ASC, `idstaff` ASC) VISIBLE,
  CONSTRAINT `fk_HTA_document_has_reviewer_HTA_document1`
    FOREIGN KEY (`idHTA_document`)
    REFERENCES `documents_db_2`.`HTA_document` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_HTA_document_has_reviewer_reviewer1`
    FOREIGN KEY (`idstaff`)
    REFERENCES `documents_db_2`.`staff` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`expert`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`expert` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `first_name` VARCHAR(45) NULL,
  `last_name` VARCHAR(45) NULL,
  `position` VARCHAR(45) NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `unique_name_position` (`first_name` ASC, `last_name` ASC, `position` ASC) VISIBLE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`HTA_document_has_expert`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`HTA_document_has_expert` (
  `idHTA_document` INT NOT NULL,
  `idexpert` INT NOT NULL,
  PRIMARY KEY (`idHTA_document`, `idexpert`),
  INDEX `fk_HTA_document_has_expert_expert1_idx` (`idexpert` ASC) VISIBLE,
  INDEX `fk_HTA_document_has_expert_HTA_document1_idx` (`idHTA_document` ASC) VISIBLE,
  CONSTRAINT `fk_HTA_document_has_expert_HTA_document1`
    FOREIGN KEY (`idHTA_document`)
    REFERENCES `documents_db_2`.`HTA_document` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_HTA_document_has_expert_expert1`
    FOREIGN KEY (`idexpert`)
    REFERENCES `documents_db_2`.`expert` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`active_drug`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`active_drug` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(200) NULL,
  `ATC` VARCHAR(45) NULL,
  `DDD` DOUBLE NULL,
  `unit` VARCHAR(45) NULL,
  `admin_route` VARCHAR(45) NULL,
  PRIMARY KEY (`id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`form`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`form` (
  `NPL_id` VARCHAR(45) NOT NULL,
  `idproduct` INT NOT NULL,
  `strength` VARCHAR(100) NULL,
  `form` VARCHAR(100) NULL,
  `MT_number` VARCHAR(45) NULL,
  `EUMA_number` VARCHAR(45) NULL,
  `earlier_name` VARCHAR(300) NULL,
  PRIMARY KEY (`NPL_id`),
  INDEX `fk_form_product1_idx` (`idproduct` ASC) VISIBLE,
  CONSTRAINT `fk_form_product1`
    FOREIGN KEY (`idproduct`)
    REFERENCES `documents_db_2`.`product` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`price`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`price` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `varunummer` INT NULL,
  `ATC` VARCHAR(45) NULL,
  `package` VARCHAR(200) NULL,
  `size` VARCHAR(45) NULL,
  `AIP` DOUBLE NULL,
  `AUP` DOUBLE NULL,
  `AIP_piece` DOUBLE NULL,
  `idcompany` INT NOT NULL,
  `NPL_id` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_price_company1_idx` (`idcompany` ASC) VISIBLE,
  INDEX `fk_price_form1_idx` (`NPL_id` ASC) VISIBLE,
  UNIQUE INDEX `unique_form_company` (`idcompany` ASC, `NPL_id` ASC) VISIBLE,
  CONSTRAINT `fk_price_company1`
    FOREIGN KEY (`idcompany`)
    REFERENCES `documents_db_2`.`company` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_price_form1`
    FOREIGN KEY (`NPL_id`)
    REFERENCES `documents_db_2`.`form` (`NPL_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`NT_council_recommendation`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`NT_council_recommendation` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `date` DATE NULL,
  `indication` VARCHAR(200) NULL,
  `ATC` VARCHAR(45) NULL,
  `recommendation` VARCHAR(45) NULL,
  `comment` VARCHAR(200) NULL,
  `active_drug` VARCHAR(200) NULL,
  `idproduct` INT NOT NULL,
  `URL` VARCHAR(300) NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_NT_council_recommendation_product1_idx` (`idproduct` ASC) VISIBLE,
  CONSTRAINT `fk_NT_council_recommendation_product1`
    FOREIGN KEY (`idproduct`)
    REFERENCES `documents_db_2`.`product` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`NT_council_deal`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`NT_council_deal` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `date` DATE NULL,
  `ATC` VARCHAR(45) NULL,
  `active_drug` VARCHAR(200) NULL,
  `recipe_type` VARCHAR(45) NULL,
  `start` DATE NULL,
  `end` DATE NULL,
  `option` DATE NULL,
  `idproduct` INT NOT NULL,
  `idcompany` INT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_NT_council_deal_product1_idx` (`idproduct` ASC) VISIBLE,
  INDEX `fk_NT_council_deal_company1_idx` (`idcompany` ASC) VISIBLE,
  UNIQUE INDEX `unique_date_product` (`date` ASC, `idproduct` ASC) VISIBLE,
  CONSTRAINT `fk_NT_council_deal_product1`
    FOREIGN KEY (`idproduct`)
    REFERENCES `documents_db_2`.`product` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_NT_council_deal_company1`
    FOREIGN KEY (`idcompany`)
    REFERENCES `documents_db_2`.`company` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`NT_council_follow_up`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`NT_council_follow_up` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `date` DATE NULL,
  `active_drug` VARCHAR(200) NULL,
  `indication` VARCHAR(200) NULL,
  `URL` VARCHAR(200) NULL,
  `idproduct` INT NOT NULL,
  `ATC` VARCHAR(45) NULL,
  `recommendation` VARCHAR(45) NULL,
  `comment` VARCHAR(45) NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_NT_council_follow_up_product1_idx` (`idproduct` ASC) VISIBLE,
  UNIQUE INDEX `unique_date_indication_product` (`idproduct` ASC, `date` ASC, `indication` ASC) VISIBLE,
  CONSTRAINT `fk_NT_council_follow_up_product1`
    FOREIGN KEY (`idproduct`)
    REFERENCES `documents_db_2`.`product` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`EMA_status`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`EMA_status` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `therapeutic_area` VARCHAR(500) NULL,
  `active_drug` VARCHAR(45) NULL,
  `active_substance` VARCHAR(45) NULL,
  `product_number` VARCHAR(45) NULL,
  `patient_safety` TINYINT NULL,
  `authorisation_status` VARCHAR(45) NULL,
  `ATC` VARCHAR(45) NULL,
  `additional_monitoring` TINYINT NULL,
  `generic` TINYINT NULL,
  `biosimilar` TINYINT NULL,
  `conditional_approval` TINYINT NULL,
  `exceptional_circumstances` TINYINT NULL,
  `accelerated_assessment` TINYINT NULL,
  `orphan_medicine` TINYINT NULL,
  `marketing_authorisation_date` DATE NULL,
  `date_of_refusal` DATE NULL,
  `human_pharmacotherapeutic_group` VARCHAR(200) NULL,
  `date_of_opinion` DATE NULL,
  `decision_date` DATE NULL,
  `revision_number` VARCHAR(45) NULL,
  `indication` TEXT NULL,
  `url` VARCHAR(150) NULL,
  `idproduct` INT NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE INDEX `product_number_UNIQUE` (`product_number` ASC) VISIBLE,
  INDEX `fk_EMA_status_product1_idx` (`idproduct` ASC) VISIBLE,
  CONSTRAINT `fk_EMA_status_product1`
    FOREIGN KEY (`idproduct`)
    REFERENCES `documents_db_2`.`product` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`product_has_active_drug`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`product_has_active_drug` (
  `idproduct` INT NOT NULL,
  `idactive_drug` INT NOT NULL,
  PRIMARY KEY (`idproduct`, `idactive_drug`),
  INDEX `fk_product_has_active_drug_active_drug1_idx` (`idactive_drug` ASC) VISIBLE,
  INDEX `fk_product_has_active_drug_product1_idx` (`idproduct` ASC) VISIBLE,
  CONSTRAINT `fk_product_has_active_drug_product1`
    FOREIGN KEY (`idproduct`)
    REFERENCES `documents_db_2`.`product` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_product_has_active_drug_active_drug1`
    FOREIGN KEY (`idactive_drug`)
    REFERENCES `documents_db_2`.`active_drug` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`regulatory_status`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`regulatory_status` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `idproduct` INT NOT NULL,
  `strength` VARCHAR(100) NULL,
  `form` VARCHAR(100) NULL,
  `status` VARCHAR(45) NULL,
  `approval_date` DATE NULL,
  `deregistration_date` DATE NULL,
  `sales_status` VARCHAR(45) NULL,
  `procedure` VARCHAR(45) NULL,
  `side_effect_spec` TINYINT NULL,
  `narcotics` VARCHAR(45) NULL,
  `exemption` TINYINT NULL,
  `prescription` VARCHAR(45) NULL,
  `country` VARCHAR(45) NULL,
  `generic` TINYINT NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_regulatory_status_product1_idx` (`idproduct` ASC) VISIBLE,
  CONSTRAINT `fk_regulatory_status_product1`
    FOREIGN KEY (`idproduct`)
    REFERENCES `documents_db_2`.`product` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`product_company`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`product_company` (
  `idcompany` INT NOT NULL,
  `idproduct` INT NOT NULL,
  `role` VARCHAR(45) NOT NULL,
  INDEX `fk_company_has_product_product1_idx` (`idproduct` ASC) VISIBLE,
  INDEX `fk_company_has_product_company1_idx` (`idcompany` ASC) VISIBLE,
  PRIMARY KEY (`idcompany`, `idproduct`, `role`),
  UNIQUE INDEX `unique_company_product_role` (`idcompany` ASC, `idproduct` ASC, `role` ASC) VISIBLE,
  CONSTRAINT `fk_company_has_product_company1`
    FOREIGN KEY (`idcompany`)
    REFERENCES `documents_db_2`.`company` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_company_has_product_product1`
    FOREIGN KEY (`idproduct`)
    REFERENCES `documents_db_2`.`product` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`HTA_document_indication`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`HTA_document_indication` (
  `idHTA_document` INT NOT NULL,
  `idindication` INT NOT NULL,
  `severity` VARCHAR(45) NULL,
  PRIMARY KEY (`idHTA_document`, `idindication`),
  INDEX `fk_HTA_document_has_indication_indication1_idx` (`idindication` ASC) VISIBLE,
  INDEX `fk_HTA_document_has_indication_HTA_document1_idx` (`idHTA_document` ASC) VISIBLE,
  CONSTRAINT `fk_HTA_document_has_indication_HTA_document1`
    FOREIGN KEY (`idHTA_document`)
    REFERENCES `documents_db_2`.`HTA_document` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_HTA_document_has_indication_indication1`
    FOREIGN KEY (`idindication`)
    REFERENCES `documents_db_2`.`indication` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`PICO`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`PICO` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `idHTA_document` INT NULL,
  `population` VARCHAR(100) NULL,
  `incidence` VARCHAR(45) NULL,
  `prevalence` VARCHAR(45) NULL,
  `severity` VARCHAR(45) NULL,
  `co_medication` VARCHAR(45) NULL,
  `intervention` VARCHAR(100) NULL,
  `comparator_company` VARCHAR(100) NULL,
  `comparator_agency` VARCHAR(100) NULL,
  `comparator_reason_company` VARCHAR(100) NULL,
  `comparator_reason_agency` VARCHAR(100) NULL,
  `comparator_modus_company` VARCHAR(100) NULL,
  `comparator_modus_agency` VARCHAR(100) NULL,
  `outcome_measure_company` VARCHAR(45) NULL,
  `outcome_measure_agency` VARCHAR(45) NULL,
  `idproduct` INT NOT NULL,
  `idindication` INT NOT NULL,
  `icd_code` VARCHAR(45) NULL,
  `indication` VARCHAR(100) NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_PICO_HTA_document1_idx` (`idHTA_document` ASC) VISIBLE,
  INDEX `fk_PICO_product1_idx` (`idproduct` ASC) VISIBLE,
  INDEX `fk_PICO_indication1_idx` (`idindication` ASC) VISIBLE,
  CONSTRAINT `fk_PICO_HTA_document1`
    FOREIGN KEY (`idHTA_document`)
    REFERENCES `documents_db_2`.`HTA_document` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_PICO_product1`
    FOREIGN KEY (`idproduct`)
    REFERENCES `documents_db_2`.`product` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_PICO_indication1`
    FOREIGN KEY (`idindication`)
    REFERENCES `documents_db_2`.`indication` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`analysis`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`analysis` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `idPICO` INT NOT NULL,
  `ICER_company` VARCHAR(45) NULL,
  `ICER_agency_lower` VARCHAR(45) NULL,
  `ICER_agency_higher` VARCHAR(45) NULL,
  `QALY_gain_company` VARCHAR(45) NULL,
  `QALY_gain_agency_lower` VARCHAR(45) NULL,
  `QALY_gain_agency_higher` VARCHAR(45) NULL,
  `QALY_total_cost_company` VARCHAR(45) NULL,
  `QALY_total_cost_agency_lower` VARCHAR(45) NULL,
  `QALY_total_cost_agency_higher` VARCHAR(45) NULL,
  `comparison_method` VARCHAR(45) NULL,
  `indirect_method` VARCHAR(45) NULL,
  `delta_cost_comp` VARCHAR(45) NULL,
  `delta_cost_agency` VARCHAR(45) NULL,
  `efficacy_summary` VARCHAR(200) NULL,
  `safety_summary` VARCHAR(200) NULL,
  `decision_summary` VARCHAR(200) NULL,
  `uncertainty_assessment_clinical` VARCHAR(45) NULL,
  `uncertainty_assessment_he` VARCHAR(45) NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_analysis_PICO1_idx` (`idPICO` ASC) VISIBLE,
  CONSTRAINT `fk_analysis_PICO1`
    FOREIGN KEY (`idPICO`)
    REFERENCES `documents_db_2`.`PICO` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`trial`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`trial` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `idanalysis` INT NOT NULL,
  `title` VARCHAR(45) NULL,
  `nr_of_patients` INT NULL,
  `nr_of_controls` INT NULL,
  `duration` VARCHAR(45) NULL,
  `phase` VARCHAR(45) NULL,
  `meta_analysis` TINYINT(1) NULL,
  `randomized` TINYINT(1) NULL,
  `controlled` TINYINT(1) NULL,
  `blinded` VARCHAR(45) NULL,
  `primary_outcome` VARCHAR(45) NULL,
  `results` VARCHAR(100) NULL,
  `safety` VARCHAR(100) NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_trial_analysis1_idx` (`idanalysis` ASC) VISIBLE,
  CONSTRAINT `fk_trial_analysis1`
    FOREIGN KEY (`idanalysis`)
    REFERENCES `documents_db_2`.`analysis` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`Costs`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`Costs` (
  `id` INT NOT NULL,
  `drug_cost` VARCHAR(45) NULL,
  `other_costs` VARCHAR(45) NULL,
  `total_treatment_cost` VARCHAR(45) NULL,
  `idanalysis` INT NOT NULL,
  `assessor` VARCHAR(45) NULL,
  `product` VARCHAR(45) NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_Costs_analysis1_idx` (`idanalysis` ASC) VISIBLE,
  CONSTRAINT `fk_Costs_analysis1`
    FOREIGN KEY (`idanalysis`)
    REFERENCES `documents_db_2`.`analysis` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `documents_db_2`.`EMA_atmp_status`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `documents_db_2`.`EMA_atmp_status` (
  `id` INT NOT NULL,
  `idproduct` INT NOT NULL,
  `type` VARCHAR(45) NULL,
  `authorisation_date` DATE NULL,
  `orphan` TINYINT(1) NULL,
  `prime` TINYINT(1) NULL,
  `withdrawal_date` DATE NULL,
  PRIMARY KEY (`id`),
  INDEX `fk_EMA_atmp_status_product1_idx` (`idproduct` ASC) VISIBLE,
  CONSTRAINT `fk_EMA_atmp_status_product1`
    FOREIGN KEY (`idproduct`)
    REFERENCES `documents_db_2`.`product` (`id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;

--
-- Table structure for table `ad_report_data`
--

DROP TABLE IF EXISTS `ad_report_data`;

CREATE TABLE `ad_report_data` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `mobile_app_resolved_id` VARCHAR(255) NOT NULL,
  `mobile_app_name` VARCHAR(255) DEFAULT NULL,
  `domain` VARCHAR(255) DEFAULT NULL,
  `ad_unit_name` VARCHAR(255) DEFAULT NULL,
  `ad_unit_id` VARCHAR(255) DEFAULT NULL,
  `inventory_format_name` VARCHAR(255) DEFAULT NULL,
  `operating_system_version_name` VARCHAR(255) DEFAULT NULL,
  `date` DATE NOT NULL,
  `ad_exchange_total_requests` BIGINT DEFAULT NULL,
  `ad_exchange_responses_served` BIGINT DEFAULT NULL,
  `ad_exchange_match_rate` DOUBLE DEFAULT NULL,
  `ad_exchange_line_item_level_impressions` BIGINT DEFAULT NULL,
  `ad_exchange_line_item_level_clicks` BIGINT DEFAULT NULL,
  `ad_exchange_line_item_level_ctr` DOUBLE DEFAULT NULL,
  `average_ecpm` DOUBLE DEFAULT NULL,
  `payout` DOUBLE DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ✅ Index definitions
CREATE INDEX idx_date ON `ad_report_data` (`date`);
CREATE INDEX idx_mobile_app_name ON `ad_report_data` (`mobile_app_name`);
CREATE INDEX idx_inventory_format ON `ad_report_data` (`inventory_format_name`);
CREATE INDEX idx_os_version ON `ad_report_data` (`operating_system_version_name`);
CREATE INDEX idx_total_requests ON `ad_report_data` (`ad_exchange_total_requests`);
CREATE INDEX idx_impressions ON `ad_report_data` (`ad_exchange_line_item_level_impressions`);
CREATE INDEX idx_clicks ON `ad_report_data` (`ad_exchange_line_item_level_clicks`);
CREATE INDEX idx_payout ON `ad_report_data` (`payout`);
CREATE INDEX idx_avg_ecpm ON `ad_report_data` (`average_ecpm`);
CREATE INDEX idx_date_app_format ON `ad_report_data` (`date`,`mobile_app_name`,`inventory_format_name`);

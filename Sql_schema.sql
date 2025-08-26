--
-- Table structure for table "ad_report_data"
--

DROP TABLE IF EXISTS "ad_report_data";

CREATE TABLE "ad_report_data" (
  "id" BIGSERIAL NOT NULL,
  "mobile_app_resolved_id" varchar(255) NOT NULL,
  "mobile_app_name" varchar(255) DEFAULT NULL,
  "domain" varchar(255) DEFAULT NULL,
  "ad_unit_name" varchar(255) DEFAULT NULL,
  "ad_unit_id" varchar(255) DEFAULT NULL,
  "inventory_format_name" varchar(255) DEFAULT NULL,
  "operating_system_version_name" varchar(255) DEFAULT NULL,
  "date" date NOT NULL,
  "ad_exchange_total_requests" bigint DEFAULT NULL,
  "ad_exchange_responses_served" bigint DEFAULT NULL,
  "ad_exchange_match_rate" double precision DEFAULT NULL,
  "ad_exchange_line_item_level_impressions" bigint DEFAULT NULL,
  "ad_exchange_line_item_level_clicks" bigint DEFAULT NULL,
  "ad_exchange_line_item_level_ctr" double precision DEFAULT NULL,
  "average_ecpm" double precision DEFAULT NULL,
  "payout" double precision DEFAULT NULL,
  PRIMARY KEY ("id"),
  CONSTRAINT unique_ad_report_record UNIQUE ("mobile_app_resolved_id", "date", "ad_unit_id", "inventory_format_name", "operating_system_version_name")
);

-- Index definitions for "ad_report_data" (converted from MySQL KEY)
CREATE INDEX idx_date ON "ad_report_data" ("date");
CREATE INDEX idx_mobile_app_name ON "ad_report_data" ("mobile_app_name");
CREATE INDEX idx_inventory_format ON "ad_report_data" ("inventory_format_name");
CREATE INDEX idx_os_version ON "ad_report_data" ("operating_system_version_name");
CREATE INDEX idx_total_requests ON "ad_report_data" ("ad_exchange_total_requests");
CREATE INDEX idx_impressions ON "ad_report_data" ("ad_exchange_line_item_level_impressions");
CREATE INDEX idx_clicks ON "ad_report_data" ("ad_exchange_line_item_level_clicks");
CREATE INDEX idx_payout ON "ad_report_data" ("payout");
CREATE INDEX idx_avg_ecpm ON "ad_report_data" ("average_ecpm");
CREATE INDEX idx_date_app_format ON "ad_report_data" ("date","mobile_app_name","inventory_format_name");
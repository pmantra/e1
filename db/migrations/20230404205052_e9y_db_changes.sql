-- migrate:up
DROP VIEW IF EXISTS member_detail_view ;

CREATE VIEW eligibility.member_detail_view AS
 SELECT m.id,
    m.organization_id,
    m.first_name,
    m.last_name,
    m.date_of_birth,
    m.work_state,
    m.email,
    m.unique_corp_id,
    m.employer_assigned_id,
    m.dependent_id,
    m.effective_range,
    m.record,
    m.file_id,
    m.do_not_contact,
    m.gender_code,
    m.created_at,
    m.updated_at,
    ma.city,
    ma.country_code,
    ma.postal_code,
        CASE
            WHEN (((lower(m.effective_range) IS NOT NULL) AND (lower(m.effective_range) <= CURRENT_DATE) AND ((upper(m.effective_range) IS NULL) OR (upper(m.effective_range) > CURRENT_DATE))) = true) THEN 'TRUE'::text
            ELSE 'FALSE'::text
        END AS active
   FROM eligibility.member m,
    eligibility.member_address ma
  WHERE (m.id = ma.member_id);

-- migrate:down

DROP VIEW IF EXISTS eligibility.member_detail_view ;

CREATE VIEW member_detail_view AS
 SELECT m.id,
    m.organization_id,
    m.first_name,
    m.last_name,
    m.date_of_birth,
    m.work_state,
    m.email,
    m.unique_corp_id,
    m.employer_assigned_id,
    m.dependent_id,
    m.effective_range,
    m.record,
    m.file_id,
    m.do_not_contact,
    m.gender_code,
    m.created_at,
    m.updated_at,
    ma.city,
    ma.country_code,
    ma.postal_code,
        CASE
            WHEN (((lower(m.effective_range) IS NOT NULL) AND (lower(m.effective_range) <= CURRENT_DATE) AND ((upper(m.effective_range) IS NULL) OR (upper(m.effective_range) > CURRENT_DATE))) = true) THEN 'TRUE'::text
            ELSE 'FALSE'::text
        END AS active
   FROM eligibility.member m,
    eligibility.member_address ma
  WHERE (m.id = ma.member_id);
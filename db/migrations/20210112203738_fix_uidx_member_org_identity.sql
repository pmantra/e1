-- migrate:up
CREATE CAST ( eligibility.member AS eligibility.org_identity )
    WITH FUNCTION eligibility.get_identity(eligibility.member) AS IMPLICIT;
ALTER INDEX IF EXISTS eligibility.uidx_member_org_identity
    RENAME TO uidx_member_identity;

-- migrate:down

DROP CAST IF EXISTS ( eligibility.member AS eligibility.org_identity );
ALTER INDEX IF EXISTS eligibility.uidx_member_identity
    RENAME TO uidx_member_org_identity;


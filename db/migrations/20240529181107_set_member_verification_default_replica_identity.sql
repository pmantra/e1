-- migrate:up transaction:false
ALTER TABLE eligibility.member_verification REPLICA IDENTITY DEFAULT;

-- migrate:down transaction:false
ALTER TABLE eligibility.member_verification REPLICA IDENTITY FULL;

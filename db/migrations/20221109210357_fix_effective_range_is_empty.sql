-- migrate:up
update eligibility.member
set effective_range = daterange(created_at::date, null)
where effective_range = 'empty'

-- migrate:down


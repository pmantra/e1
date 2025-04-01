-- migrate:up
DELETE FROM public.schema_migrations WHERE version = '20210318184307';

-- migrate:down
INSERT INTO public.schema_migrations (version) VALUES ('20210318184307');

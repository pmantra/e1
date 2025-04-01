
v0.10.4 / 2021-02-11
====================

  * Hotfix: Fix record deletion

v0.10.3 / 2021-02-10
====================

  * Merge branch 'sean/ch27108/optimize-persistence-consumers' into 'master'
  * Use git repo mmlib
  * Fix content hash for mmlib.
  * v0.10.3
  * Update `asyncpg` for better `executemany` performance and `typical` for more-compliant email validation.
  * Use `MGET` when bulk fetching rows from Redis to reduce network roundtrips to 1.
  * Add a changelog.

v0.10.2 / 2021-02-09
====================

  * Hotfix: Fix redis connection handling and clearing error reports in action.py

v0.10.1 / 2021-02-09
====================

  * Hotfix: Use the correct redis DSN for Action publishing.

v0.10.0 / 2021-02-09
====================

  * Merge branch 'sean/ch26899/e9y-admin-isn-t-propagating-persistence-messages' into 'master'
  * v0.10.0
  * Use connection pool for fanning out publishing messages.
  * Drop legacy look up for action indexes.
  * Offload scheduling persistence to a stream worker instead of view handler/thread-pool in admin.

v0.9.2 / 2021-02-09
===================

  * Add logging and make action-handling a staticmethod for passing into the thread pool [ch26899]

v0.9.1 / 2021-02-08
===================

  * Merge branch 'sean/ch26843/handle-headers-with-line-breaks' into 'master'
  * Handle Headers with Line Breaks [ch26843]

v0.9.0 / 2021-02-08
===================

  * Merge branch 'sean/ch26795/bad-gateway-error-in-e9y-admin' into 'master'
  * v0.9.0
  * Abstract fetching pending results into the storage client and add an additional index for file+action.
  * Offload scheduling actions into a separate thread.
  * Merge branch 'jaelee/fix-tilt' into 'master'
  * Make dev enabled by default
  * Cleanup and add comments for Streams runner.
  * whoops - monkey patch settings in the e9y session....

v0.8.1 / 2021-02-05
===================

  * v0.8.1
  * Fix tests for local dev and further db client optimizations/fixes.

v0.8.0 / 2021-02-05
===================

  * Merge branch 'sean/ch26456/jitter-stream-worker-reads' into 'master'
  * Minor optimizations for persistence workers and bump the version.
  * Increase the timeout for persistence workers.
  * Pull in up to `count` of pending messages, and then only add new entries if we need.
  * Only re-raise CancelledError from stream handler if the timeout has expired.
  * Allow workers to run in parallel in pycharm.
  * Jitter Stream Worker Reads [ch26456]

v0.7.9 / 2021-02-04
===================

  * Merge branch 'sean/ch26445/use-client-specific-keys-in-pii-check' into 'master'
  * Use Client Specific Keys in PII Check [ch26445]

v0.7.8 / 2021-02-04
===================

  * Hotfix: Further reduce batch size for writing to pg.
  * Hotfix: Don't try to parse null/empty values for country codes.

v0.7.7 / 2021-02-04
===================

  * Hotfix: Use more-lenient `to_date` in `row_to_member`

v0.7.6 / 2021-02-03
===================

  * Hotfix: Reduce batch size for writes.
  * Revert: Use `tini` to ensure the app isn't run with PID=1.
  * Hotfix: Use `tini` to ensure the app isn't run with PID=1.
  * Hotfix: Manually cancel all outstanding tasks on shutdown.
  * Hotfix: Raise a RuntimeError if we get a CancelledError but the stream is still marked to run.

v0.7.5 / 2021-02-03
===================

  * Hotfix: Always run the shutdown handler when Streams.run_async() exits.
  * Merge branch 'sean/ch25531/build-out-e9y-test-suite' into 'master'
  * More deterministic upsert checks.
  * Fleshing out test suite for db client and fixing related bugs.
  * Use a script to wait for services to come online
  * Don't set search_path in DATABASE_URL
  * Fix migrations.Dockerfile entrypoint
  * Add the necessary env vars to get our test services running and allow dbmate to handle migrations
  * Fix functional-tests job needs.
  * Start building CI and test harness for functional tests.
  * Add unit tests for translator functions.

v0.7.4 / 2021-02-03
===================

  * Hotfix: Make error handling a lot noisier.

v0.7.3 / 2021-02-01
===================

  * Drop refernces to `primary_key` in configuration upserts [ch25622]
  * Merge branch 'sean/ch25490/drop-duplicates-from-e9y-member-table' into 'master'
  * Drop duplicates from e9y member table in migration [ch25490]

v0.7.2 / 2021-01-28
===================

  * Hotfix: Ensure we map legacy header fields to new header fields.
  * Merge branch 'sean/ch24685/state-country-errors-should-be-a-warning' into 'master'
  * Relax country & state validation.
  * Merge branch 'sean/ch23826/eliminate-email-primary-key-for-census-files' into 'master'
  * Update schema/migrations to re-define `org_identity` without the `email` field.
  * Allow empty emails when parsing files.
  * Drop `Configuration.primary_key` and all support for `email` as an alternative org pk.
  * Merge branch 'andrewmoffat/ch20366/e9y-admin-is-not-reporting-exceptions' into 'master'
  * Merge branch 'andrewmoffat/ch25013/e9y-prod-is-reporting-all-exceptions-as-service' into 'master'
  * actually report the exception
  * eligibility protobufs always reformatting
  * quote the keys to make syntax highlighters happy
  * use real mmlib package
  * Merge branch 'sean/ch25267/header-alias-table-has-empty-aliases' into 'master'
  * Make sure we maintian an accurate set of header aliases and clean out any empty aliases from the db.
  * Merge branch 'master' into andrewmoffat/ch25013/e9y-prod-is-reporting-all-exceptions-as-service
  * cleanup debugging
  * testing pubsub exc
  * cleaning up cruft
  * exc from log msg
  * Hotfix: We don't have the file object in _check_files_complete
  * Merge branch 'master' into andrewmoffat/ch25013/e9y-prod-is-reporting-all-exceptions-as-service
  * disable cloud_emulation
  * updating mmlib for branch
  * testing exceptions in qa1
  * make it clear whose errors are whose in cloud emulation mode
  * cloud emulation requires config overrides
  * dont short circuit on no devlocal, since we may be doing cloud emulation
  * improvements
  * more remote debugger mappings
  * bare exception
  * testing errors on qa1

v0.7.1 / 2021-01-27
===================

  * Patch: Add `filename` key to persistence logging.
  * Patch: If project == "local-dev", use local storage.
  * Merge branch 'sean/ch23817/e9y-hardening-and-tuning-continued' into 'master'
  * Merge branch 'andrewmoffat/ch25207/e9y-tests-shouldn-t-rely-on-dev-mode-being' into 'master'
  * disable local dev and have tests pass
  * Clean up `Stream.next()` and log whenever iteration breaks if it shouldn't have done so.
  * Re-enable dead-letter monitoring.
  * Use a lock when shutting down the app and track shutdown state rather than ignoring specific exceptions in the loop exception handler.
  * Hotfix: Fix record serialization for BQ export.
  * Merge branch 'andrewmoffat/ch19725/allowlist-flask-logger-in-eligibility-admin' into 'master'
  * more comments
  * Hotfix: Don't capture/wrap standard exit signals
  * frickin lint plz
  * fix config file live updates
  * better tilt live updates
  * allowlist werkzeug, for flask admin request logging

v0.6.2 / 2021-01-26
===================

  * Merge branch 'sean/ch24970/e9y-keeping-transactions-open-to-mono-db' into 'master'
  * v0.6.2
  * If we receive an exception without any message or context, force the app to fail loudly.
  * autocommit MySQL transactions to ensure we don't lock the `organization` table.
  * Merge branch 'andrewmoffat/ch24706/don-t-allow-deploying-e9y-if-rehome-image' into 'master'
  * prod and qa were backwards
  * make prod deployment build dependent on additional jobs
  * Merge branch 'andrewmoffat/ch24703/local-mmlib-dev-improvements' into 'master'
  * dont error on unbound var
  * comment about mmlib token security
  * its a variable
  * make correct dir
  * debugging build
  * fix directory location
  * reset default debug level back to what it was
  * no need to prune minikube now that we're using docker_prune_settings
  * local mmlib
  * Merge branch 'master' into feature/testing-error-reporting
  * local mmlib progress
  * Merge branch 'master' into feature/testing-error-reporting
  * cruft cleanup
  * Revert "tests"
  * Revert "allow demo schemas"
  * mmlib 0.6.4
  * Cruft cleanup
  * updates
  * sleep
  * content hash?
  * updates
  * use deploy token
  * build attempt
  * mmlib local dev
  * use https for mmlib
  * update deps
  * testing mmlib branch
  * Revert "remove local_mmlib for now"
  * allow demo schemas
  * build chart should be implied if chart version is specified
  * black exclude
  * make linting happy
  * remove local_mmlib for now
  * Merge branch 'master' into feature/testing-error-reporting
  * updates
  * no longer needed
  * test output
  * tests

v0.6.1 / 2021-01-22
===================

  * Merge branch 'sean/hotfix/log-and-force-app-to-close' into 'master'
  * v0.6.1
  * Don't catch bare exceptions.
  * Handle null values for state fields gracefully.
  * Export data to BQ using models.
  * Merge branch 'sean/hotfix/log-and-force-app-to-close' into 'master'
  * Raise logging to warn when ignoring exception.
  * Add extra logging withing Stream.next and catch/raise bare exceptions.
  * Hotfix: Remove duplicated member entries before re-creating unique identity index.

v0.6.0 / 2021-01-22
===================

  * Merge branch 'sean/ch23817/e9y-hardening-and-tuning-continued' into 'master'
  * Use connection helpers in admin action handler.
  * Drop use of `org_identity` for locating members to delete - we don't need it.
  * v0.6.0
  * Update member identity index to use correct operators on queries.
  * Strip whitespace from dependent ids.
  * Don't crash the app if we can't publish to BQ for some reason.
  * Bail out of completion checker if there are no completed files.
  * Add better large file simulation to local test files.
  * comment for exported apis
  * autovacuum for postgres
  * Merge branch 'chore/redis-persistence' into 'master'
  * re-enable redis persistence on leader
  * build chart should be implied if chart version is specified
  * Hotfix: Increase batch size for writes to 1,000 [ch23817]

v0.5.5 / 2021-01-19
===================

  * Merge branch 'sean/ch23721/detail-key-organization-id-is-not-present' into 'master'
  * v0.5.5
  * Persist the configuration *before* the header mapping when receiving a new file.
  * Merge branch 'andrewmoffat/ch23377/ensure-e9y-can-be-deployed-to-qa2' into 'master'
  * Merge branch 'feature/naming-tweaks' into andrewmoffat/ch23377/ensure-e9y-can-be-deployed-to-qa2
  * fixes
  * Cleanup and better logging
  * better iam naming convention
  * preferred naming convention
  * bugfix
  * bad sub name
  * use qa env
  * subs need to have unique names too
  * missing chart name
  * unused import
  * need another suffix
  * Merge branch 'master' into andrewmoffat/ch23377/ensure-e9y-can-be-deployed-to-qa2
  * updates
  * updates
  * docs
  * testing
  * break it up
  * set -e
  * qa-env
  * Revert "bifurcated gitlab job"
  * bifurcated gitlab job

v0.5.4 / 2021-01-15
===================

  * Hotfix: Limit connection pool size on worker startup and label connections with app/facet name.

v0.5.3 / 2021-01-15
===================

  * Merge branch 'sean/thread-safe-connections' into 'master'
  * If asyncio isn't present, Python is shutting down and we can't import it anyway. Just exit `__del__`
  * Bring back connection limits...
  * Ensure we clear thread-local state on app shutdown and get rid of privately-managed connections in admin views.
  * Update mmlib.
  * Admin is now threaded.
  * Pubsub worker no longer needs to create/teardown connections for every message.
  * Make connection pools thread-safe.

v0.5.2 / 2021-01-14
===================

  * v0.5.2
  * Hotfix: Initialize redis and pg connection pools at app startup and cache in thread-local storage.

v0.5.1 / 2021-01-14
===================

  * Hotfix: Set default min connection pool size to 2
  * Hotfix: Undo full delete in header_alias migration (we don't want this to happen if the migration is run again for some reason).
  * Hotfix: delete all aliases in db during header alias migration.
  * Merge branch 'sean/fix-header-alias-migration' into 'master'
  * Fix header_alias migration.

v0.5.0 / 2021-01-14
===================

  * Merge branch 'sean/ch20775/create-pipeline-export-from-eligibility-service' into 'master'
  * Fix bulk_delete operation-type.
  * Automatically retry database operations on recoverable errors.
  * Limit pool size for pubsub worker and admin
  * Don't fuck with the search_path in the migrations.
  * data export should come in from TF
  * in qa1, data export topic is diff
  * publish permissions to data export topic
  * sometimes subs won't exist
  * refactor data export topic into its own config field
  * Bump VERSION.txt
  * Update mmlib
  * Optimize org_identity difference query.
  * Add `explain()` helper to db client.
  * Add support for exporting processing data to Big Query upon file completion.
  * Add client, PK, & admin view for `header_alias` table.
  * Merge branch 'andrewmoffat/ch22923/add-prod-deploy-button-to-gitlab-pipelines' into 'master'
  * allow overriding deploy env
  * parameterize deploy env
  * missing a needs for some env artifacts
  * using docker? need dind
  * Merge branch 'andrewmoffat/ch20316/make-deploybot-deploy-eligibility-qa-and' into 'master'
  * Revert "we don't need helm here"
  * we don't need helm here
  * dont need secrets for tests
  * straightening up the makefile
  * dind it
  * fixes
  * updates
  * help
  * update
  * updates
  * updates
  * fixes
  * updates
  * comments and restructuring gitlab
  * coalesce
  * do it all
  * propagate app version
  * deploy chart
  * updates
  * Multiline
  * no sleep
  * updates
  * updates
  * updates
  * relative artifacts
  * removing sleep
  * artifacts
  * debugging
  * fixing chart version
  * helm install
  * helm install
  * use env
  * new stage
  * updates
  * updates
  * updates
  * updates
  * deploy chart
  * fixing yaml
  * updates
  * experimenting
  * Merge branch 'master' into andrewmoffat/ch20316/make-deploybot-deploy-eligibility-qa-and
  * Add organization_id to Member handler
  * Merge branch 'reverted-work-after-0192c30b' into 'master'
  * updates
  * updates
  * updates
  * updates
  * updates
  * tests
  * update
  * deploy qa
  * comments
  * resetting search path after migration
  * Merge branch 'andrewmoffat/ch20814/refactor-e9y-version-source' into 'master'
  * Merge branch 'master' into 'andrewmoffat/ch20814/refactor-e9y-version-source'
  * fixing tiltfile to pull in version
  * now that the migrations job can end on its own, don't limit it by time
  * v0.4.14
  * Fix querying for header mappings.
  * Fix paginated redis scanning.
  * Add a local test-file based on legit BofA data.
  * updates to makefile
  * Revert "Alias the version field in pyproject.toml during our docker builds."
  * turn down the request/limits for default facets, we can customize in terraform
  * Merge branch 'reverting-0192c30b' into 'master'
  * Revert "turn down the request/limits for default facets, we can customize in terraform"
  * Revert "fixing tiltfile to pull in version"
  * resetting search path after migration
  * Merge branch 'andrewmoffat/ch20814/refactor-e9y-version-source' into 'master'
  * Merge branch 'master' into 'andrewmoffat/ch20814/refactor-e9y-version-source'
  * fixing tiltfile to pull in version
  * now that the migrations job can end on its own, don't limit it by time
  * updates to makefile
  * Revert "Alias the version field in pyproject.toml during our docker builds."

v0.4.14 / 2020-12-22
====================

  * v0.4.14
  * Fix querying for header mappings.
  * Fix paginated redis scanning.
  * Add a local test-file based on legit BofA data.
  * turn down the request/limits for default facets, we can customize in terraform
  * Fix kwd-arg `chunk -> batch`
  * explicitly disable aof, since we don't have persistence anyways
  * Merge branch 'andrewmoffat/ch20795/disable-leader-follower-persistence-in-e9y' into 'master'
  * disable persistence

v0.4.13 / 2020-12-22
====================

  * Run saving of a batch within its own method to optimize for GC.
  * doubling memory reqs
  * disable autoscaling
  * Extract the field mapping from the correct key in Mono Organization.
  * Filter org identity difference by org ID :bug:

v0.4.12 / 2020-12-22
====================

  * Use batching + MSET for writing results.
  * Don't block-read for handler timeoutms

v0.4.11 / 2020-12-22
====================

  * v0.4.11
  * Use MSET for atomic bulk writes.

v0.4.10 / 2020-12-21
====================

  * v0.4.10
  * Batch writes to redis in chunks of 1000.

v0.4.9 / 2020-12-21
===================

  * v0.4.9
  * Timeout should be applied when yielding in `Stream.next()`, not on the handler, which runs forever.
  * redis single master configuration

v0.4.8 / 2020-12-21
===================

  * v0.4.8
  * Make timeoutms configurable per handler, and actually *timeout the handler* when it goes over the configured value.

v0.4.7 / 2020-12-21
===================

  * v0.4.7
  * Add a migration to clear out all development data from qa & prod.
  * Inject the current file id for members to delete and schedule a completion check for the submitted file(s) in the delete_record handler.
  * Fix form processing for IncompleteFilesView.
  * secret_key should be capitalized for app config.

v0.4.6 / 2020-12-21
===================

  * Flash a message after scheduling actions and add a debug log.
  * Merge branch 'andrewmoffat/ch20503/helm-chart-app-version-not-updating-when' into 'master'
  * use new app_version
  * use chart version

v0.4.5 / 2020-12-18
===================

  * v0.4.5
  * Add some more logging to Admin and fix action form.
  * Remove unused code-paths/queries for db client and fix notations for delete operations.
  * Use `0.0.0` as version alias for Dockerfiles.

v0.4.4 / 2020-12-18
===================

  * v0.4.4
  * Alias the version field in pyproject.toml during our docker builds.
  * Maintain an internal cursor when scanning for redis entries.

v0.4.3 / 2020-12-18
===================

  * Field names for queries should always be lower.

v0.4.2 / 2020-12-18
===================

  * Add logging to Redis table view.
  * enable redis flush commands

0.4.1 / 2020-12-18
==================

  * Strip quote chars (`'`, `"`) from headers.
  * Add support for purging all parsed records for a file.

v0.3.3 / 2020-12-18
===================

  * We don't need any math magic for pagination to work.

v0.3.2 / 2020-12-18
===================

  * Fix filtering files by status and loading config headers.

v0.3.1 / 2020-12-18
===================

  * Records to delete may not have an `errors` key.

v0.3.0 / 2020-12-18
===================

  * Get counts and filtering working on Admin.
  * Update mmlib.

v0.2.3 / 2020-12-17
===================

  * Clean up Incomplete Files View UI

v0.2.2 / 2020-12-17
===================

  * Fix redis index backfill.

v0.2.1 / 2020-12-17
===================

  * v0.2.1
  * Convert records to dicts for dumping to Redis so they're not saved as json arrays.
  * Merge branch 'sean/ch20367/parsedrecordstorage-indexed-queries' into 'master'
  * Split the key correctly when scheduling deletes.
  * Use a single connection pool for handling actions.
  * `MultiDict.lists()` is a generator.
  * Make `incomplete_by_org` a mapping instead of a groupby object.
  * Fan out count operations into separate tasks.
  * Handle organizations with no members when fetching counts [ch20370]
  * Only translate rows which are mappings in redis view [ch20370]
  * use redis-master, since headless actually points to the RO replica
  * Merge branch 'andrewmoffat/ch20350/set-resource-request-limits-for-e9y' into 'master'
  * resource limits
  * facet resources
  * 2 admins for good measure
  * connect to redis headless, not master, so that failovers work
  * Merge branch 'andrewmoffat/ch20349/add-main-nodeselector-label-to-remaining' into 'master'
  * node selector
  * set redis node selector
  * Merge branch 'master' into andrewmoffat/ch20349/add-main-nodeselector-label-to-remaining
  * redis insights and migration job node selector

v0.1.2 / 2020-12-17
===================

  * Merge branch 'sean/ch20336/eligibility-tracing-and-metrics-are-erroring' into 'master'
  * use default mmlib and mmfactory versions (instead of null) in tilt
  * Bump version
  * Update mmlib to support nested exception reporting context.
  * Put error reporting within the tracing/metrics context and pass the span to the error reporting.

v0.1.1 / 2020-12-17
===================

  * Merge branch 'sean/tune-admin-view' into 'master'
  * Add a hook to check if a file is completed and set the completed_at timestamp if so.
  * Merge branch 'andrewmoffat/ch20313/subsequent-helm-chart-deploys-when-using' into 'master'
  * Tune queries and operations for IncompleteFilesView and add lots of logging.
  * keep helm hook resources around, also run them on upgrade
  * Only show orgs with files which have parsed records.
  * Make sure we're cleaning up our redis connections in the redis view.
  * Remove placholder views/templates in Admin.
  * run the two rehomes in parrallel
  * add more tags
  * tag before pushing
  * push with the tag
  * ls the images
  * rehome the migrations image too
  * Handle missing dob when translating a record to a member object (may happen with error records).
  * Improve connection pooling and lifecycle for workers.
  * push properly
  * bring the job back
  * comment this stuff out for now
  * switch to foundation-build-image base
  * Point app at the correct host in QA and Prod
  * use the .dind base
  * use the BUILD_TAG
  * print before running
  * user docker image pull + docker image push to rehome
  * Merge branch 'sean/ch16924/eligibility-implement-an-admin-view-to-review' into 'master'
  * actually run the job step
  * Get basic filtering on database views.
  * Merge branch 'dual-home-final-builds' into 'master'
  * code final release builds to qa image bucket too
  * Get Actions working.
  * Merge branch 'jaelee/fix-demo-issue' into 'master'
  * Update gitignore to commit generated schemas
  * Remove DemoService
  * Merge branch 'setup-env-vars' into 'master'
  * add setup-env-vars to make a build.env file
  * Merge branch 'master' of gitlab.mvnapp.net:maven/eligibility-api
  * tripwire and fixing helm labels which cant have plusses
  * fully bisect this ci file
  * Merge branch 'raph/ch20158/eligibility-gitlab-ci-should-push-non-master' into 'master'
  * Merge branch 'master' into 'raph/ch20158/eligibility-gitlab-ci-should-push-non-master'
  * generate the BUILD_TAG faster without dependencies
  * Merge branch 'sean/bare-test-image' into 'master'
  * Get a very basic overview bootstrapped.
  * use environments to switch variable values
  * Run poetry installation in separate command from app dependencies.
  * Turn off Klar scanning for test image
  * Start building out overview page.
  * test image doesn't require app image
  * Reduce layers.
  * Copy hollow secrets for tests.
  * Hollow out test.Dockerfile.
  * Merge branch 'andrewmoffat/ch19847/make-gitlab-ci-push-migrations-image' into 'master'
  * tag latest
  * reordering build
  * fixing
  * testing
  * fixing gitlab
  * build test docker image off of app docker image @Raph
  * cleanups
  * updates
  * Merge branch 'jaelee/finish-eligibility-handlers' into 'master'
  * Finish eligibility handlers
  * test
  * updates
  * testing
  * Use the same key-name for `filename` in logging.
  * Reduce the amount of passed state: only send a File.id to downstream listeners of `pending-files` stream.
  * testing
  * testing
  * testing
  * testing
  * Merge branch 'sean/ch16924/eligibility-implement-an-admin-view-to-review' into 'master'
  * testing
  * testing builds
  * Reorder params
  * Add filters to Redis View.
  * Merge branch 'jaelee/tilt-stuff' into 'master'
  * Move PyCharm debugger to base
  * Set the correct db host
  * Get pagination working for Redis view.
  * Shape redis records like Member objects for review.
  * ignore some directories for tilt dev
  * use project dir for debug config
  * dont leak gitlab token to screen
  * Merge branch 'andrewmoffat/ch17717/enable-tilt-live-updates-for-eligibility' into 'master'
  * tilt live updates
  * Attempt to fix QA1
  * Flesh out some implementation for Redis/Database views.
  * Fix Admin connection management.
  * Always return a date object in `convert.to_date()` [ch20059]
  * Merge branch 'master' of gitlab.mvnapp.net:maven/eligibility-api
  * no quotes
  * Merge branch 'andrewmoffat/ch19901/include-code-sha-as-part-of-the-app-s-semver' into 'master'
  * better handling of chart version
  * linting
  * version tag improvements
  * deployment help args
  * better liveness for redisinsight
  * Add eligibility grpc handlers
  * Merge branch 'andrewmoffat/ch19849/upon-building-an-eligibility-deployment-bundle' into 'master'
  * build metadata semver format
  * unused configs
  * this file was automatically generated by
  * mmlib is in a repo now, no need for this
  * scrub confusing settings
  * Merge branch 'fix/refactoring-deployment' into 'master'
  * pass in bucket project if not bucket owned
  * allow extra .gitattributes file for lfs charts
  * Merge branch 'feature/remote-debugger' into 'master'
  * no bare exception
  * tilt debugger
  * alt way of getting gitlab password
  * cleanup unneeded job
  * pycharm debugger connections
  * shoudl be building '-app' image
  * fix linting
  * Merge branch 'master' of gitlab.mvnapp.net:maven/eligibility-api
  * output apis
  * helm pre-install hooks
  * ignore all secret prefix in config
  * improved template output
  * pass image project into system
  * speed up formatting by doing it in bulk
  * optionally build chart
  * pass non-owned buckets into tf module via vars
  * make image building more general
  * Fix issue with node selector
  * fixes
  * switch interface based on dev mode
  * Merge branch 'andrewmoffat/ch19730/disable-eligibility-admin-iap-guard' into 'master'
  * make linter happy
  * disable iap for now
  * Merge branch 'raph/ch19712/proxy-eligibility-admin' into 'master'
  * setup a k8s service for admin
  * Set root url for Admin so we can proxy to Mono admin host.
  * Merge branch 'jaelee/ch19379/migrations-for-eligibility-app-are-busted' into 'master'
  * Try without RUN
  * Remove unused Dockerfile
  * WIP
  * Update templates
  * Merge branch 'andrewmoffat/ch19491/wire-up-kek-and-signing-key-permissions-to' into 'master'
  * make flake8 happy
  * Merge branch 'jaelee/ch19410/set-up-eligibility-prod-config-overlay' into 'master'
  * Add prod.yml config
  * Merge branch 'master' into andrewmoffat/ch19491/wire-up-kek-and-signing-key-permissions-to
  * Read from the correct bucket in QA.
  * making customization templates work
  * Fix entrypoints/commands for our Dockerfiles.
  * Update Makefile with gitlab PyPI token.
  * Merge branch 'jaelee/green-ci-builds' into 'master'
  * Jaelee/green ci builds
  * Fix: get tests running again with all the changes & add run config.
  * Fix: Use correct keys when sanitizing metadata and remove config/logic for saving encrypted files
  * Merge branch 'master' into andrewmoffat/ch19491/wire-up-kek-and-signing-key-permissions-to
  * Merge branch 'fix/bad-signing-key-reference' into 'master'
  * removing trailing whatever, which isn't part of the key id
  * beginnings
  * @apopp requests using this node selector
  * Including `directory_name` in persistence query.
  * owned may not exist on the bucket config
  * Merge branch 'feature/allow-pubsub-from-other-buckets'
  * allow pubsub to be driven by a bucket in another project
  * Merge branch 'sean/ch19388/add-directory-name-to-config-table' into 'master'
  * Add a helper class for scheduling persistence and clearing errors from redis.
  * Fix iteration and cancellation of `db.redis.Scanner`.
  * Merge branch 'andrewmoffat/ch19407/wire-up-encryption-qa-params-real-census' into 'master'
  * real qa configs
  * Tag `Configurations.get_by_directory_name` as coerceable.
  * Add unique `directory_name` column to configuration schema.
  * Persisting: point tmp storage at the correct address.
  * Processing: point tmp storage at the correct address.
  * GCS: Fix fat-finger.
  * GCS: Handle un-encrypted files.
  * PubSub: ignore changes to folder objects.
  * Crypto: await the result of validating metadata.
  * bucket object read perms
  * Set qa mono_db.host to localhost, add a make recipe for port-forwarding redisinsight
  * nodeselectors via @apopp
  * Merge branch 'master' of gitlab.mvnapp.net:maven/eligibility-api
  * node-type for migration
  * Merge branch 'sean/ch19235/add-redisinsight-to-deployment' into 'master'
  * Add a service definition and hostAliases to the deployment.
  * Add a helper function for scheduling persistence actions to a stream [ch19284]
  * Add initial helm/Tilt config for redisinsight
  * Fixes for local-dev with bucket config updates.
  * Merge branch 'feature/cloud-sql-mono-db' into 'master'
  * make mono db connection accessible to cloud sql
  * Merge branch 'sean/ch16921/fixes' into 'master'
  * Update lockfile
  * Pin GCP Logging Client, since 2.0 isn't actually compatible with Error Reporting [ch19172]
  * Scrub redis DSN from logging, reprs [ch19073]
  * Only require the redis key for persistence actions
  * Merge branch 'fix/mono-db-config'
  * fixing mono_db config structure
  * Merge branch 'andrewmoffat/ch13074/basic-terraform-module-for-eligibility-service'
  * mmlib version bump
  * Merge branch 'master' into andrewmoffat/ch13074/basic-terraform-module-for-eligibility-service
  * updates
  * Merge branch 'sean/ch16921/fixes' into 'master'
  * Turn off dead-lettering in Redis Streams for now.
  * Fixes for querying and persisting data.
  * Add support for querying mono, get pubsub and streams working together.
  * updates
  * updates
  * Merge branch 'master' into andrewmoffat/ch13074/basic-terraform-module-for-eligibility-service
  * updates
  * Fix connection pool creation and flask helpers.
  * Merge branch 'master' into andrewmoffat/ch13074/basic-terraform-module-for-eligibility-service
  * updates
  * Merge branch 'sean/ch16921/fix-ddl' into 'master'
  * Move global PG types into the eligibility schema (GCP disallows direct management of pg_catalog).
  * refactorings deployment
  * merge configs
  * makefile fixes
  * Merge branch 'andrewmoffat/ch13074/basic-terraform-module-for-eligibility-service'
  * updates
  * Merge branch 'master' into andrewmoffat/ch13074/basic-terraform-module-for-eligibility-service
  * redis
  * Merge branch 'master' of gitlab.mvnapp.net:maven/eligibility-api into andrewmoffat/ch13074/basic-terraform-module-for-eligibility-service
  * dont use private db
  * checkpoint
  * updates
  * don't auto create
  * updates
  * updates
  * updates
  * removing very important threes
  * cleanup
  * Merge branch 'master' into andrewmoffat/ch13074/basic-terraform-module-for-eligibility-service
  * Merge branch 'master' into andrewmoffat/ch13074/basic-terraform-module-for-eligibility-service
  * updates
  * updates
  * updates
  * some service account and cloudsql related things
  * Merge branch 'master' into andrewmoffat/ch13074/basic-terraform-module-for-eligibility-service
  * Merge branch 'master' into andrewmoffat/ch13074/basic-terraform-module-for-eligibility-service
  * fixing secrets template

v0.1.0 / 2020-12-05
===================

  * Merge branch 'sean/ch16921/eligibility-implement-a-consumer-for-parsing' into 'master'
  * Implement tests with local test files.
  * Implement country/state normalization.
  * Some fixups for scripts and docs
  * Add the local storage to the app Dockerfile
  * Runner fixups.
  * Update & pin some dependencies.
  * Handle data orjson can't gracefully when writing keys.
  * Allow for passing in an external connection for db clients.
  * Clean up GCS configuration in base command.
  * More fine-grained logging based on verbosity
  * Make the pubsub runner just a handoff to the Streams runner (issues with event loop).
  * Add a LocalStorage fake for GCS interactions.
  * Update PyCharm run configurations.
  * Get the app running with a local gcs emulator
  * Merge branch 'sean/ch16921/eligibility-implement-a-consumer-for-parsing' into 'master'
  * Don't `return` in file processor! Just continue.
  * Cleaned up logging and added comments explaining processor branching.
  * Implement consumer to delete member records [ch16923]
  * Implement consumer to persist member records [ch16922]
  * Implement file processing consumer [ch16921]
  * FileManager may return bytes OR str
  * Add a configuration for gcs.bucket.
  * Merge branch 'feature/allowlist-logging' into 'master'
  * be a little more aggressive about dep logging filtering
  * Move temporary storage modules into `db`.
  * Merge branch 'sean/ch16921/eligibility-implement-a-consumer-for-parsing' into 'master'
  * only output dep logging if you ask for it
  * Port GCS and Encryption from Mono.
  * Implement temporary storage for parsed eligibility files.
  * Initial file processor implementation [ch16921].
  * Improvements for development: documentation, run configurations, run commands, etc.
  * Remove "eligibility" entry from internal packages.
  * Add `make init-local` command to check for external dependencies and intall the application.
  * Add the formatter commit to the git blame ignore.
  * Run black and isort on source.
  * Add pre-commit hook configuration.
  * Merge branch 'sean/ch17910/noisy-eligibility-logging-regression' into 'master'
  * Filter out `google` loggers by default.
  * Merge branch 'andrewmoffat/ch17909/eligibility-not-passing-correct-facet-into' into 'master'
  * send facet name into instrumentation
  * Merge branch 'andrewmoffat/ch13074/basic-terraform-module-for-eligibility-service' into 'master'
  * updates
  * green lights
  * formatting
  * settings refactor
  * fixing template loading
  * Merge branch 'master' into andrewmoffat/ch13074/basic-terraform-module-for-eligibility-service
  * updates
  * Don't delete "dead-letter" messages when cleaning up pending messages.
  * Tweaks to CLI for startup banner and customizable subtitle.
  * Merge branch 'sean/ch17369/worker-add-asyncio-runners' into 'master'
  * Migrate CLI to cleo commands.
  * Some general clean up for constants & logging.
  * Migrate pubsub/streams to internal runners and get them talking to each other.
  * Add/implement asyncio runners for redis streams and google pubsub.
  * checkpoint
  * Merge branch 'master' into andrewmoffat/ch13074/basic-terraform-module-for-eligibility-service
  * checkpoint
  * Merge branch 'andrewmoffat/ch13611/flask-admin-error-reporting-not-using-mmlib' into 'master'
  * attach error reporting to flask admin exceptions
  * updates
  * updates
  * updates
  * ignore
  * checkpoint
  * checkpoint
  * checkpoint
  * checkpoint
  * Merge branch 'master' into andrewmoffat/ch13074/basic-terraform-module-for-eligibility-service
  * checkpoint
  * Merge branch 'sean/ch16925/eligibility-stamp-out-required-work-for-processing' into 'master'
  * Stamp out file-processing & admin work [ch16925]
  * chore(db): drop sqlalchemy code and add flask helpers for admin.
  * chore: use dotenv instead of a yaml file.
  * chore: rename ui -> admin
  * Merge branch 'sean/ch15311/adjust-eligibility-schema-to-support-custom' into 'master'
  * fix(ddl): Ensure we reset the search path to "public" at the end of the create_globals migration.
  * checkpoint
  * checkpoint
  * checkpoint
  * refactoring
  * Merge branch 'sean/ch15311/adjust-eligibility-schema-to-support-custom' into 'master'
  * Add a file id reference to the member table.
  * Fix bugs with eligibility member querying
  * Adjustments for more dynamic eligibility schema.
  * Merge branch 'sean/ch14621/pipes-wire-up-eligibility-service-for-redis' into 'master'
  * Wire Up Eligibility Service for Redis [ch14621]
  * Small cleanups for db clients
  * Merge branch 'sean/ch14664/foundation-write-query-library' into 'master'
  * Add models and service clients which handle defined models
  * Update schema for Member table to gracefully handle potential null values in the unique index.
  * Reference `foundation.yml` in .gitlab-ci.yml [ch12351]
  * Implement basic query library.
  * Updates to the schema to reflect correct use-cases
  * Wire up dbmate + Postgres + basic schema.
  * Move sqlalchemy session management to dedicated module.
  * better filtering out the breaking mmlib line in requirements.txt
  * yaml loader warning
  * patching some things
  * first commit

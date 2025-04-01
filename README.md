# -Eligibility API-

- A high-level overview of Eligibilty (E9Y) can be found [here](https://www.notion.so/mavenclinic/Eligibility-e9y-97fc43a08b224434a418844145622ed4)
- A technical overview on local E9Y development can be found [here](https://www.notion.so/mavenclinic/Developing-on-e9y-32464453552046ce8df7c66c7617dc93)

## Repo Structure
### Top Level Directory Layout

    .
    ├── .storage         # Sample census files
    ├── api              # GRPC files
    ├── app              # Core application logic
    ├── bin              # Application entrypoint
    ├── config           # Config files, environment variables
    ├── db               # Model declarations, DB client code for e9y database and e9y mono database
    ├── dockerfiles      # Dockerfiles used to build e9y images
    ├── scratch          #
    ├── scripts          #
    ├── tests            # Pytests, broken into functional and unit tests
    └── README.md        #

### Lower Level App Directory Layout

    .
    ├── ...
    ├── app                 
    │   ├── admin          # 
    │   ├── api            #
    │   ├── common         #
    │   ├── eligibility    #
    │   ├── tasks          #
    │   ├── workers        # google pubsub, redis workers, dataclasses
    │   └── __init__.py    # Getting started guide
    └── ...

> 📋 `/app/worker/redis.py`
> 
> This file will hold declarations of our various redis consumers, but the actual code related to standing up these workers and the redis queue lives in the [mmstream repo](https://gitlab.mvnapp.net/maven/mmstream).


## Database


### Seeding the DBs
There are two files we use to seed our local DB data, located under the `scripts/db_setup` dir.

Once you have completed local setup and can view both your e9y-api DB and e9y-mono DB, you should copy the SQL within those files
and execute it within a DB shell or GUI tool. The data seeded should be enough to help you perform the bulk of the operations needed
for day-to-day development.

### Running Migrations
* `dbmate` is used to handle all of our migrations. Details can be found in [this RFC](https://www.notion.so/mavenclinic/Provisional-Migrations-with-dbmate-6fc083c757084b9d9de15c54ab2d998f).

### Database Migrations

If you need to perform a database migration, please satisfy the following prerequisites and follow the steps detailed below. 

#### Prerequisites

- Install `postgresql`
    
    ```bash
    brew install postgresql
    ```
    
- Install [dbmate](https://github.com/amacneil/dbmate)
    
    ```bash
    brew install dbmate
    ```
    
- Start local postgresql DB via `docker-compose`. Follow command [here](https://www.notion.so/Developing-on-e9y-32464453552046ce8df7c66c7617dc93).

#### Create a Migration

1. Set the postgresql DB connection string as an environment variable. 
    
    ```bash
    export DATABASE_URL="postgresql://postgres:@eligibility-db/api?sslmode=disable"
    ```
    
2. Create a new migration file where `<my_new_migration>` is the name of your new migration.
    
    ```bash
    dbmate new <my_new_migration>
    ```
    
    If successful, the command should output the path of the new migration file, verify that the file is where it should be.
    
    ```bash
    Creating migration: db/migrations/20220528123839_my_new_migration.sql
    ```
    
3. Add your SQL to your generated migration file, and save the file.
    
    ```sql
    -- migrate:up
    DROP TABLE IF EXISTS fruits;
    CREATE TABLE fruits (
      id INTEGER,
      name TEXT,
      color TEXT
    );
    
    -- migrate:down
    DROP TABLE fruits;
    ```
    
4. Run the migration.
    
    ```sql
    dbmate migrate
    ```
    
    You should see the following output if the migration was successful.
    
    ```bash
    Applying: 20220528123839_my_new_migration.sql
    Writing: ./db/schema.sql
    ```
    
    Verify that the `schema.sql` file was correctly modified with the following addition.
    
    ```sql
    --
    -- Name: fruits; Type: TABLE; Schema: public; Owner: -
    --
    
    CREATE TABLE public.fruits (
        id integer,
        name text,
        color text
    );
    ```
    
> 💡 `dbmate migrate` should run `dbmate dump` which creates/updates the `schema.sql` file. But this will fail silently if the necessary `pg_dump` is not installed via `brew install postgresql`

> Once you have confirmed your migration looks great, add it to its own branch and create an MR only with the migration in it.

**✨ Voilà ✨ You just ran your first DB migration!**


## Data Sources for E9Y
There are 3 ways we can receive data for E9Y
- Kafka
- File Processing
- Third Party API

### Kafka
Currently, only one client makes use of our kafka integration -> Optum.

The logic lives within the `eligibility-integrations` repo. Within that repo, we set up a Kafka consumer, which ingests incoming e9y data from Optum.

This data goes through some lightweight normalization, and is then sent to e9y-api via Google PubSub messages. These records are then handled and parsed by our PubSub worker-
specifically in the  [external_record_notification_handler](https://gitlab.mvnapp.net/maven/eligibility-api/-/blob/main/app/worker/pubsub.py#L130)

This handler will work to further normalize and clean the incoming data and save it to our PostGres DB. After we have normalized it, a copy
will be [sent to BigQuery](https://gitlab.mvnapp.net/maven/eligibility-api/-/blob/main/app/worker/pubsub.py#L159:164) for reference usage.

### File Processing

Files are a major source of our eligibility information. When a customer first signs up with Maven, they will configure an SFTP bucket to drop off Eligibility files to.

#### File Processing Dataflow ####
1. Client drop off files on our SFTP server [FTP receiver](https://gitlab.mvnapp.net/maven/ftp_receiver/-/tree/master/ftp_server)
2. Files are upload to a GCP Storage Bucket via our [FTP uploader](https://gitlab.mvnapp.net/maven/ftp_receiver/-/tree/master/ftp_uploader)
3. Once a file has been uploaded to the storage bucket, GCP [automatically](https://cloud.google.com/storage/docs/pubsub-notifications) publishes a PubSub message. This notification system is configured [here](https://gitlab.mvnapp.net/maven/infrastructure/-/blob/master/systems/maven-legacy/eligibility/gcs.tf).
4. Once we receive a Pubsub notification, we ingest the file and begin processing it [here](https://gitlab.mvnapp.net/maven/eligibility-api/-/blob/master/app/worker/pubsub.py#L41).
5. Based on the directory where the file originated from, we determine what organization's configuration to pull before we parse the file. This configuration is [syned](https://gitlab.mvnapp.net/maven/eligibility-api/-/blob/master/app/tasks/sync.py) from the Mono MySQL database.
6. Once we have made record of the file and have done the bare minimum of ingestion, we notify our `pending-file` redis stream which [reads in and normalizes the file.](https://gitlab.mvnapp.net/maven/eligibility-api/-/blob/master/app/worker/redis.py#L36)
7. The records and any errors encountered during parsing are then persisted in our temporary staging tables `eligibility.file_parse_results` and `eligibility.file_parse_errors`. If they are below the error threshold for review, they will be flushed for storage in `eligibility.members`, if they are above the threshold, they will be held in those tables until Client Delivery can review them via admin. The logic for this process can be found [here](https://gitlab.mvnapp.net/maven/eligibility-api/-/tree/master/app/eligibility/domain).

Once files have been picked up, we are notified via pubsub message that they have been dropped off for later processing. This PubSub message is actually [automatically
generated for us by GCP](https://cloud.google.com/storage/docs/pubsub-notifications). This notification system is configured [here](https://gitlab.mvnapp.net/maven/infrastructure/-/blob/main/systems/maven-modern/eligibility/gcs.tf).

Once we receive a notification from Google, we ingest the file and begin processing it [here](https://gitlab.mvnapp.net/maven/eligibility-api/-/blob/main/app/worker/pubsub.py#L41).

There *is* some interaction with Mono in our file processing- for example when we pull the organization information associated with the bucket the file was originally in.

Once we have made record of the file and have done the bare minimum of ingestion, we notify our `pending-file` redis stream which [reads in and normalizes the file.](https://gitlab.mvnapp.net/maven/eligibility-api/-/blob/main/app/worker/redis.py#L36)

The records within the file are then passed around and saved to our DB as needed.

### Third Party API
Currently, the only customer using a third-party API to provider eligibility information is Microsoft.
When a Maven member attempts to log in for the first time using their MSFT credentials, we will send an API request
to [Microsoft to verify their eligibility](https://gitlab.mvnapp.net/maven/eligibility-api/-/blob/main/app/eligibility/client_specific/microsoft.py)

This relies on the [ClientSpecificService abstraction layer](https://gitlab.mvnapp.net/maven/eligibility-api/-/blob/main/app/eligibility/client_specific/service.py#L24) - it currently only supports microsoft, but was written to support future expansion.

This entire flow is managed by the EligibilityService [(specific call for client specific calls is here)](https://gitlab.mvnapp.net/maven/eligibility-api/-/blob/main/app/eligibility/service.py#L103) , which is managed by [this handler code](https://gitlab.mvnapp.net/maven/eligibility-api/-/blob/main/app/api/handlers.py#L50)

This all bubbles up from our GRPC server who exposes [the call](https://gitlab.mvnapp.net/maven/eligibility-api/-/blob/main/api/protobufs/generated/python/maven_schemas/eligibility_grpc.py#L81) to check client specific eligibility.

Payloads are based off our [protobuf schema](https://gitlab.mvnapp.net/maven/eligibility-api/-/blob/main/api/protobufs/generated/python/maven_schemas/eligibility_pb2.py) .

For clients to be able to use the API, they need to make use of our [GRPC stub](https://gitlab.mvnapp.net/maven/eligibility-api/-/blob/main/api/protobufs/generated/python/maven_schemas/eligibility_grpc.py#L114) - a great example of the usage pattern of an external client with our stub is [here](https://gitlab.mvnapp.net/maven/maven/-/blob/main/api/common/services/eligibility/service.py).


If this request times out or has errors, we *do* fall back to eligibility information provided by Microsoft in a file (ingested in the normal file processing process)

MSFT authentication requires a private key and a certificate. To run the application locally, you'll need to set the following environment variables:
- MSFT_PRIVATE_KEY_PATH: The file path to your private key.
- MSFT_CERTIFICATE_PATH: The file path to your certificate.

You can find these in [1Password](https://start.1password.com/open/i?a=LWPXYD67GVBKNK4P3UGD37G23M&v=uucbucpz6hh3qvuulegsq557ja&i=2mamyj65lb5o6nfxnwo2ivx4ge&h=maven.1password.com)


## Mono
Unfortunately, there is still some E9Y logic living within mono.
We are working to move logic out of mono and into e9y-api, but until that project has been completed, you may need to reference the repository.

A quick lay of the land for e9y code in mono:
- File processing: This is where we receive files for e9y, attempt to open them, and then send a notification that they are ready for use by e9y api

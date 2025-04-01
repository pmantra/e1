### Making Changes to the Protobuf Schema

1. Clone the [maven-schemas](https://gitlab.mvnapp.net/maven/maven-schemas) repository to a location outside of this project directory, make a note of the absolute path of where you cloned the repo. Mine was `/Users/peisenxue/Projects/maven-schemas`.


2. From the current directory, `api/protobufs`, we will want to create symbolic link between a `maven-schemas` directory to the absolute path of `maven-schemas` from step 1. You can do that with the command below.
```shell
ln -s /Users/peisenxue/Projects/maven-schemas maven-schemas
```

3. Confirm that the symlink was successfully created with `ls -al`. You should see the output below. 
```shell
(.venv) peisenxue@mvnmac-536 protobufs % ls -al
total 24
drwxr-xr-x  7 peisenxue  staff  224 Aug  4 15:08 .
drwxr-xr-x  5 peisenxue  staff  160 Jun 29 16:51 ..
-rw-r--r--  1 peisenxue  staff  140 Apr 29 14:12 .gitignore
-rw-r--r--  1 peisenxue  staff  184 Apr 29 14:12 Makefile
-rw-r--r--  1 peisenxue  staff  671 Aug  4 15:08 README.md
drwxr-xr-x  3 peisenxue  staff   96 Apr 29 14:12 generated
lrwxr-xr-x  1 peisenxue  staff   39 Aug  4 14:50 maven-schemas -> /Users/peisenxue/Projects/maven-schemas
```

4. Make any necessary schema changes to the `*.proto` file.

5. To compile your new changes, run `make` in this directory. Verify that changes were made to the files in `api/protobufs/generated/python/maven_schemas`.

6. Be sure to commit your changes in the `maven-schema` repo.


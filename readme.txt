in order to properly function, the init_index.sh script must be ran with at least
one book in the hdfs under folder /TinyGoogle/books afterwards all web
functionality will automatically self enable

the site front end can be found at tinygoogle.jakehalloran.com however searching is unlikely to work
as I will be momentarily taking the data nodes offline for funding reasons

In case you want to build the site yourself, it is designed to be run on a basic LAMP stack

Compilation of spark jar was done with maven with spark and hadoop dependencies pom file is attached.

Note: using upload functionality can make re-initializing index necessary if the job fails as the existing index will silently be destroyed.

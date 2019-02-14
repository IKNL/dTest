# dTest
Proof of concept R-project for running distributed learning algorithms using a central container.

The process for running/using the code consists of the following steps:
 1. Run `build_docker.sh` to build the docker image and upload it to the registry used for distributed learning
 1. Load the R-code from `main.R` and run the method `run.master(username, password, collaboration_id, host, api_path)` with the appropriate parameters.
 
 

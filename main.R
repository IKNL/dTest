# Simple script to test running algorithms in a central container.

# Load libraries/scrips
source("Client.R")

# Global constants
IMAGE = "docker-registry.distributedlearning.ai/dl_test"

# ******************************************************************************
# ---- Utility functions ----
# ******************************************************************************

#' Write a string to STDOUT without the standard '[1]' prefix.
writeln <- function(x="", sep=" ") {
    cat(paste(paste(x, collapse=sep), "\n"))
}

#' Load text from disk
loadText <- function(filename) {
    input_data <- readChar(filename, file.info(filename)$size)
    return(input_data)
}

#' Load JSON from disk
loadJSON <- function(filename) {
    input_data <- fromJSON(loadText(filename))
    return(input_data)
}

# ******************************************************************************
# ---- Remote procedures/functions ----
# ******************************************************************************

#' Compute some statistics on the data
RPC_compute_statistic <- function(df) {
    # return('this is a statistic ;-)')
    return(runif(1, 1, 10))
}

# ******************************************************************************
# ---- Infrastrucure functions (site code) ----
# ******************************************************************************

#' Run the local (site) code
docker.node <- function(input_data) {
    writeln("Running as node!")

    # Load data from CSV
    database_uri <- Sys.getenv("DATABASE_URI")
    writeln(sprintf("Using '%s' as database", database_uri))
    df <- read.csv(database_uri)

    writeln("Dispatching ...")
    result <- dispatch_RPC(df, input_data)

    return(result)
}

#' Run the method requested by the server
#'
#' Params:
#'   df: data frame containing the *local* dataset
#'   input_data: string containing serialized JSON; JSON should contain
#'               the keys 'method', 'args' and 'kwargs'
#'
#' Return:
#'   Requested method's output
dispatch_RPC <- function(df, input_data) {
    # Determine which method was requested and combine arguments and keyword
    # arguments in a single variable
    method <- sprintf("RPC_%s", input_data$method)

    input_data$args <- readRDS(textConnection(input_data$args))
    input_data$kwargs <- readRDS(textConnection(input_data$kwargs))

    args <- c(list(df), input_data$args, input_data$kwargs)

    # Call the method
    writeln(sprintf("Calling %s", method))
    result <- do.call(method, args)

    # Serialize the result
    writeln("Serializing result")
    fp <- textConnection("result_data", open="w")
    saveRDS(result, fp, ascii=T)
    close(fp)
    result <- result_data
    writeln("Serializing complete")

    return(result)
}

# ******************************************************************************
# ---- Infrastrucure functions (master code) ----
# ******************************************************************************

#' Run the central (algorithm orchestration) code
docker.master <- function(input_data) {
    writeln("Running as master!")

    writeln("Loading token from disk")
    token <- loadText('token.txt')
    token <- trimws(token)

    writeln("Creating client")
    client <- Client(
        host=input_data$host,
        access_token=token,
        collaboration_id=input_data$collaboration_id,
        api_path=input_data$api_path
    )

    writeln("Calling RPC compute_statistic")
    results = call_parallel(client, IMAGE, 'compute_statistic')

    writeln("Serializing to JSON and returning results!")
    return(toJSON(results))
}

#' Wait for the results of a distributed task and return the task,
#' including results.
#'
#' Params:
#'   client: ptmclient::Client instance.
#'   task: list with the key id (representing the task id)
#'
#' Return:
#'   task (list) including results
wait_for_results <- function(client, task) {
    path = sprintf('/task/%s', task$id)

    while(TRUE) {
        r <- client$GET(path)

        if (content(r)$complete) {
            break

        } else {
            # Wait n seconds
            writeln("Waiting for results ...")
            Sys.sleep(5)
        }
    }

    path = sprintf('/task/%s?include=results', task$id)
    r <- client$GET(path)

    return(content(r))
}

#' Execute a method on the distributed learning infrastructure.
#'
#' This entails ...
#'  * creating a task and letting the nodes execute the method
#'    specified in the 'input' parameter
#'  * waiting for all results to arrive
#'  * deserializing each sites' result using readRDS
#'
#' Params:
#'   client: ptmclient::Client instance.
#'   method: name of the method to call on the distributed learning
#'           infrastructure
#'   ...: (keyword) arguments to provide to method. The arguments are serialized
#'        using `saveRDS()` by `create_task_input()`.
#'
#' Return:
#'   return value of called method
call_parallel <- function(client, image, method, database='', ...) {
    # Create the json structure for the call to the server
    input <- create_task_input(method, ...)

    task = list(
        "name"="RPC call",
        "image"=image,
        "collaboration_id"=client$get("collaboration_id"),
        "input"=input,
        "database"=database,
        "description"=""
    )

    # Create the task on the server; this returns the task with its id
    writeln("POSTing task to sever")
    r <- client$POST('/task', task)

    # Wait for the results to come in
    result_dict <- wait_for_results(client, content(r))

    # result_dict is a list with the keys _id, id, description, complete, image,
    # collaboration, results, etc. the entry "results" is itself a list with
    # one entry for each site. The site's actual result is contained in the
    # named list member 'result' and is encoded using saveRDS.
    sites <- result_dict$results
    results <- list()

    for (k in 1:length(sites)) {
        results[[k]] <- readRDS(textConnection(sites[[k]]$result))
    }

    return(results)
}

#' Create a data structure used as input for a call to the distributed
#' learning infrastructure.
create_task_input = function(method, ...) {
    # Construct the input_data list from the ellipsis.
    arguments <- list(...)

    if (is.null(names(arguments))) {
        args <- arguments
        kwargs <- list()

    } else {
        args <- arguments[names(arguments) == ""]
        kwargs <- arguments[names(arguments) != ""]
    }

    # Serialize the argument values to ASCII
    fp <- textConnection("arg_data", open="w")
    saveRDS(args, fp, ascii=T)
    close(fp)

    # Serialize the keyword argument values to ASCII
    fp <- textConnection("kwarg_data", open="w")
    saveRDS(kwargs, fp, ascii=T)
    close(fp)

    # Create the data structure
    input_data <- list(
        method=method,
        args=arg_data,
        kwargs=kwarg_data
    )

    return(input_data)
}


# ******************************************************************************
# ---- RStudio entry point ----
# ******************************************************************************
run.master <- function(username, password, collaboration_id,
                       host="localhost:5000", api_path="/api") {
    client <- Client(
        host=host,
        username=username,
        password=password,
        collaboration_id=collaboration_id,
        api_path=api_path
    )
    client$authenticate()

    task = list(
        name="RPC call",
        image=IMAGE,
        collaboration_id=collaboration_id,
        input=list(
            role="master",
            host=host,
            api_path=api_path,
            collaboration_id=collaboration_id
        ),
        database="",
        description=""
    )

    # Create the task on the server; this returs the task with its id
    r <- client$POST('/task', task)

    # Wait for the results to come in
    result_dict <- wait_for_results(client, content(r))
    sites <- result_dict$results
    results <- list()

    for (k in 1:length(sites)) {
        results[[k]] <- fromJSON(sites[[k]]$result)
    }

    return(results)
}


# ******************************************************************************
# ---- RScript entry point ----
# ******************************************************************************

#' Entrypoint when excecuting this script using Rscript
#'
#' Deserialization/serialization is performed in `dipatch_RPC()` to enable
#' testing.
docker.wrapper <- function() {
    # Read the contents of file input.txt into 'input_data'
    writeln("Loading input.txt ...")
    input_data <- loadJSON('input.txt')

    if (!is.null(input_data$role) && input_data$role == "master") {
        result <- docker.master(input_data)

    } else {
        result <- docker.node(input_data)

    }

    # Write result to disk
    writeln("Writing result to disk .. ")
    writeLines(result, "output.txt")

    writeln("")
    writeln("[DONE!]")
}

# ******************************************************************************
# ---- main() ----
# ******************************************************************************
if (!interactive()) {
    docker.wrapper()
}


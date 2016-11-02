# DynamoDB Presto-Plugin
A plugin to query DynamoDB tables with SQL queries from Presto. With the help of Presto, the plugin translates SQL statements to parallel table scans via the DynamoDB API.

Some highlights of the plugin:

- Automatic detection of DynamoDB table "schemas".
- Parallel scans on DynamoDB tables distributed across a Presto cluster.
- Mapping of DynamoDB data to Presto-SQL and Java datatypes.

## Prerequisites

- Git client
- Maven build processor


## Setup Guide

The setup consists of multiple steps: (1) building, (2) preparation, (3) execution. Each step is described in the following.

### Building

The plugin is embedded into the Presto source code and build process. To build the plugin together with Presto, checkout the [source code](../) with ```git```:

```bash
git clone https://github.com/mugglmenzel/presto
``` 

In the main folder of the source code, use Maven to build the code. We can skip tests to speed up the build tasks:

```bash
mvn clean install -Dmaven.test.skip=true
```

### Preparation

There are three options to prepare Presto to run with the DynamoDB plugin:

- Install the plugin into an existing Presto server.
- Use the RPM version of the Presto server with the plugin.
- Use the compiled version of the Presto server with the plugin.

The options are explained in the following.

All options require the placement of configuration files. Templates with default contents of the files can be found in the [defaults folder](../defaults).

#### Existing Presto Server

First, get the compiled ZIP file from the [target](./target) folder in the present directory. There should only be one ZIP file.

Then, unpack the ZIP file into your existing Presto server (and all workers) under ```[presto-server-folder]/plugins```. Rename the folder to ```dynamo```.

Lastly, copy the [DynamoDB plugin configuration file](../defaults/catalog/dynamo.properties) to the ```[presto-server-folder]/etc/catalog/``` folder where all plugin config files reside. Make sure the file is called ```dynamo.properties```. Adapt the file as needed.

#### RPM Version

The RPM-version of the Presto server already includes the DynamoDB plugin. However, it needs to be configured before it can start up successfully.

Install the RPM server version as described in [the presto-server-rpm module](../presto-server-rpm/).

Copy the [DynamoDB plugin configuration file](../defaults/catalog/dynamo.properties) to ```/etc/presto/catalog/``` before starting the Presto server. Adapt the file as needed.

####  Compiled Server Version

The compiled Presto server already includes the DynamoDB plugin. However, it needs to be configured before it can start up successfully.

Fortunately, the DynamoDB plugin includes a helper script which prepares a folder with a setup Presto server. Run the following script with ```bash``` as the following example shows:

```bash
bash presto-dynamo/prepare-presto.sh
```

You can now adapt the plugin configuration file at ```dist/etc/dynamo.properties``` in the source code folder.

### Execution

As for the preparation part, there are three options to run Presto with the DynamoDB plugin:

- Run an existing Presto server with the plugin.
- Run the RPM version of the Presto server with the plugin.
- Run the compiled version of the Presto server with the plugin.

The following sections describe the three alternativ setups in detail.

#### Existing Presto Server

If you are using your own Presto version, you should already know how to launch Preso. If not, please follow the [documentation at prestodb.io](https://prestodb.io/docs/current/installation/deployment.html).

#### RPM Version

The RPM version of the Presto server describes how to launch the server in [its own guide](../presto-server-rpm/). Typically, you need to execute ```service presto start``` with the RPM version.

#### Compiled Server Version

With the compiled server version, you need to launch the server using the launcher located at ```dist/bin/launcher```.
The following command is enough to start the server as is:

```bash
dist/bin/launcher run
```
or

```bash
dist/bin/launcher start
```

You can find more details in the [documentation at prestodb.io](https://prestodb.io/docs/current/installation/deployment.html).

## Configuration

Parameters of the plugin can be configured in the ```dynamo.properties``` file.
The following table lists all active configuration parameters of the plugin. 

| PROPERTY | DEFAULT | DESCRIPTION |
|:---------|--------:|:------------|
| dynamo.fetch-size | 5000 | Records fetched in each scan request. |
| dynamo.split-size | 20000 | Records allocated to each worker for processing. |
| dynamo.min-split-count | 2 | Minimum parallelization factor (can be greater than workers). |
| dynamo.schema-cache-ttl | 1.00h | Frequence of the DynamoDB "schema" detection. |



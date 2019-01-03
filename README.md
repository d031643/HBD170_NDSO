# Data Warehousing Project
## Overview
This is a Data Warehousing Project:
A [Data Warehousing](https://en.wikipedia.org/wiki/Data_warehouse) project has three default modules:

- A Persistence module, which is an [HDB Module](https://www.youtube.com/watch?v=gNGn1dq4IMM) and contains the definitions of the required [database model](https://en.wikipedia.org/wiki/Database_model)
  - [datastore objects](https://help.sap.com/doc/DRAFT/5f67aa7f9a8b491cb08ea4bf9d381d90/2.0.0.0/en-US/SAP_HANA_DWF_Native_DataStore_Object.pdf)
  - [flowgraphs](https://help.sap.com/viewer/DRAFT/1e4f857a22aa477081d41d3b6fa48d99/2.0.0.0/en-US/bd68e24d1d744727891d4955640445f1.html)
  -  [calculation views](http://saphanatutorial.com/sap-hana-modeling/)
  - [persmissions] (https://github.wdf.sap.corp/xs2/hdideploy.js) that are defined in the defaults folder within the src folder.

- The Task Chain module, which is a Data Warehousing Foundation module contains the definitions of the task chains and their schedules. This is quite like the [BW Process Chains](https://help.sap.com/saphelp_nw70/helpdata/en/8f/c08b3baaa59649e10000000a11402f/content.htm).

- The Backend module, which is a [node module](https://nodejs.org/api/modules.html) provides [REST](https://en.wikipedia.org/wiki/Representational_state_transfer) access to the datastore objects defined in the persistence module.
  This module implements two task groups; ndso and flowgraph.
  + The ndso task group has task types that:
    - Activate NDSO: this allows to activate all requests of a given datastore
    - Load from File: this allows to write the content of a csv file from the file system to a specified inbound table
    - Load from URL: this allows to retrieve a csv source from the Web and write it to a specified inbound table
    - Load via SQL: this allows to write the result set of a query to a specified inbound table

  + The FlowGraph task group has the task type that: 
    - Execute Flowgraph: this allows to execute a Flowgraph without parameters

Use the registerTaskType.js file to register new task groups.
To register new task types of a task group you need to create a new route in the Router.js file and adjust the routes. For example; "router.get( '/MyOwnTask/v1', TaskChain.getTaskTypesMyOwnTask );" for the task type "MyOwnTask".

## Development Model
The following [developing model](https://en.wikipedia.org/wiki/Software_development_process) is proposed:


###Environment Setup
As a development environment, SAP Web IDE should be used.
Every developer should have a user assigned to him for the Web IDE and a space exclusivly assigned to him as well.
In this space the di-builder should be deployed, which can be done with the space-enablement ui of di-core.

### Initializing Project
The developers can now create projects based on the Data Warehousing project template. As a space
for this project, they should use their exclusivly owned space. They should use the  "fixed service names" flag,
which can be maintained in the project wizards template customization step.


### Editing the Sources
- Task chains: During editing of task chains, the backend module must be running.
  Only then the task types are properly registered and the value help becomes available.
- DB Artifact: DB artifacts are to be placed in the persistence module.
  There, the src folder is the location of choice.
  - The datastore object should be used for storing all topic and time-based information.
  - Flowgraphs should be used for extraction, transformation and loading.
   To use the value help, the developer needs to rebuild the persistence module whenever he has created
   new catalog artifacts.
  - Analytical views should used to publish the data to OLAP clients.
  - Analytical privileges should be used to restrict access to the data.

### Sharing Sources
The sources of the project should be checked in to a versioning management system.
Web IDE offers a good integration to
GitHub. SAP Web IDE offers to clone, pull, commit and push projects and project artifacts to [github](https://github.com/).

### Building and Running the Project
To test the project, developers can run the build on project level. The result is an mtar file, which can be
 exported to the file system and then deployed to
the test system via the command line tool "xs", say via xs deploy <mtarName>.


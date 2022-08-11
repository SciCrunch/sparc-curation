# About Simple SCKAN

* **Simple SCKAN** refers to an extension of the SCKAN's NPO ontology that incorporates simplified relations to support simpler queries on basic connectivity knowledge available in NPO using SPARQL query language. The basic connevtivity relations that we thought would be useful are the soma, axon, and dendrite locations, as well as axon terminal and axon sensory terminal locations. Here is the list of simplified relational predicates available in Simple SCKAN:
	* _**hasSomaLocation**_: a relation between a Neuron type and its soma location 
	* _**hasAxonLocation**_: a relation between a Neuron type and its axon location  
	* _**hasDendriteLocation**_: a relation between a Neuron type and its dendrite location 
	* _**hasAxonTerminalLocation**_: a relation between a Neuron type and its axon terminal location 
	* _**hasAxonSensoryLocation**_: a relation between a Neuron type and its sensory axon terminal location
* Simple SCKAN Example Queries
	* Link will be posted here.     
* You can download **Simple SCKAN** from the following link and load it into any standard Graph Databese such as Stardog, Graph DB, or Neo4J.
	* [**Downlod Simple SCKAN from here**](https://github.com/SciCrunch/NIF-Ontology/releases/download/sckan-2022-08-pre-2/sckan-npo-simple.ttl).
* We recommend using **Stardog Studio** as the SPARQL query interface to query over the NPO/SCKAN connectivity knowledge
    * A Stardog server is set up by the FDI lab to support the **Stardog Stuio** interface for the users like SCKAN's NLP curators to check whether a connection exists between regions based on a set of neuron populations in NPO
    * We have a set of predefined "canned" query patterns avialable via Stardog that we think would be useful for the NLP curators and other interested users.
* What to expect in the future:
    * Have the Simple SCKAN graph available via SciGraph so that the SciGraph users can develop different tools and apps based on simpler queries using SPARQL or Cypher.
    * We also want to have the Stardog interface available for broader audience who would prefer a simpler way to query SCKAN connectivity knowledge.

# Running Simple SCKAN Queries in Stardog

*	Contact [FDI Lab](https://www.fdilab.org/contact) for the username and password to access the FDI Lab's Stardog server
*	Load **Stardog Studio** from your web browser (Chrome works better)
    * Link: https://stardog.studio/#/
*	Do the following to connect to our FDI Lab’s Stardog server:
	* Press the **Red Power Button** at the bottom-left corner of the page
	* On **Connect to Stardog** window, select **New Connection** and enter the following: 
		* Username: Your username
		*	Password: Your password
		*	Endpoint: https://stardog.scicrunch.io:5821
	*	Click **Add to My Connections** checkbox 
		*	Enter a **Name** for your connection (e.g., SPARC-USER)
*	Once connected, do the following:
	*	Click on the **Workspace** icon at the top-left corner
		*	Simply click **All Databases** dropdown box and select **NPO**
	* Select one of the **STORED** queries under NPO
		*	Run the query by pressing the **Blue Run Button** on the right panel
		* The comments in the queries should provide information on how to customize the queries

# SCKAN Vs. SIMPLE-SCKAN
* The underlying ontologies for SCKAN are developed using stadrad OWL-based formalism. The two main reasons for choosing OWL-based formalism are the following: (a) defining the neuron types with precise logical axioms and quantifiers (b) utilizing stadard reasoners for automated classification of  neuron types, detecting inconsistencies in the asserted logical axioms, and inferring implicit axioms from the explicit ones. 
	* While OWL provides a useful formalism to specify the logical axioms of an ontology in a rigorous manner, it lacks the support for quiring and retreiving those axiom-level knowledge. There is simply not enough tool-support available for complex axiom-level queries that one could use against the OWL ontologies. 
	* The graph-based standard query laguages such as SPARQL and CYPHER are mostly meant for writing data or individual-level queries against a knowledgebase (comibination of ontological classes and their instantiations with actual instance data). 
		* While one can use SPARQL or Cypher to retreive axiom-level knowledge from an OWL ontology, it usually requires an extensive amount of knowledge both about the domain and the structure of the ontology, as well as the technical jargon of the OWL/RDF formalisms. In fact, OWL class-level queries with complex logical axioms can be extremely difficult and tedious to write and test using SPARQL or Cypher. 
	* Since the neuron types in NPO/SCKAN ontologies are defined based on a set of OWL logical axioms, SCKAN suffers the exact same pitfalls above when it comes to retreiving the neuronal connectivity knowledge from its ontologies. 
* **SIMPLE-SCKAN** refers to an extension of the SCKAN that incorporates a set of simplified _**subject-predicate-object**_ relations about the basic connectivity knowledge available in NPO. The key objective of the **SIMPLE-SCKAN** is to **_support simplicity_** while writing the queries against NPO's axiom-level connectivity knowledge. The basic connevtivity relations that we thought would be useful are the soma, axon, and dendrite locations, as well as axon terminal and axon sensory terminal locations. Here is the list of simplified relational predicates or properties available in SIMPLE-SCKAN:
	* _**hasSomaLocation**_: a relation between a Neuron type and its soma location 
	* _**hasAxonLocation**_: a relation between a Neuron type and its axon location  
	* _**hasDendriteLocation**_: a relation between a Neuron type and its dendrite location 
	* _**hasAxonTerminalLocation**_: a relation between a Neuron type and its axon terminal location 
	* _**hasAxonSensoryLocation**_: a relation between a Neuron type and its sensory axon terminal location
* The relational properties above in **SIMPLE-SCKAN** encapsulates the actual ontological connectivity axioms in NPO. In other words, SIMPLE-SCKAN converts the complex OWL axioms from NPO into simple RDF graph patterns for the sole purpose of querying and retrieiving SCKAN's connectivity knowledge in a much simpler manner.
* You can download **SIMPLE-SCKAN** from the following link and load it into any standard Graph Databese such as Stardog, Graph DB, or Neo4J.
	* [**Downlod Simple SCKAN from here**](https://github.com/SciCrunch/NIF-Ontology/releases/download/sckan-2022-08-pre-2/sckan-npo-simple.ttl).
* We recommend using **Stardog Studio** as the SPARQL query interface to query over the NPO/SCKAN connectivity knowledge
    * A Stardog endpoint is set up by the FDI lab to support the **Stardog Stuio** interface for the users like SCKAN's NLP curators to check whether a connection exists between regions based on a set of neuron populations in NPO
    * We have a set of predefined "canned" query patterns avialable via Stardog that we think would be useful for the NLP curators and other interested users.
* What to expect in the future:
    * Have the Simple SCKAN graph available via SciGraph so that the SciGraph users can develop different tools and apps based on simpler queries using SPARQL or Cypher.
    * We also want to have the Stardog interface available for broader audience who would prefer a simpler way to query SCKAN connectivity knowledge.


# Running Simple SCKAN Queries in Stardog

*	Contact [FDI Lab](https://www.fdilab.org/contact) for the username and password to access the FDI Lab's Stardog endpoint
*	Load **Stardog Studio** from your web browser (Chrome works better)
    * Link: https://stardog.studio/#/
*	Do the following to connect to our FDI Lab’s Stardog endpoint:
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

# **Simple-SCKAN** Example Queries
	* Link will be posted here.
